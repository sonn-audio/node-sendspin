import type { IncomingMessage } from 'node:http';
import { URL } from 'node:url';
import WebSocket from 'ws';

import {
  ConnectionReason,
  GoodbyeReason,
  PlaybackStateType,
  ServerCommandPayload,
  SourceSignalType,
  SourceStateType,
  type RoleName,
} from '../types.js';
import { SendspinSession, type SendspinSessionHooks, type SendspinPcmFrame, type SendspinConnectionMeta, type PlayerFormat } from './session.js';

/**
 * Core Sendspin session manager: tracks WebSocket sessions and routes server-driven messages.
 */
/** Ping every connected socket on this cadence; a socket that doesn't pong
 *  before the next sweep is considered dead and terminated (so ≈30–60s to
 *  reap). Mirrors the reference server's aiohttp `heartbeat=30`. Browsers (and
 *  the node `ws` client) reply to control-frame pings automatically, so a live
 *  tab stays alive without any client code — only genuinely-dead sockets
 *  (crashed tab, vanished network, force-quit) fail to pong and get reaped. */
const HEARTBEAT_INTERVAL_MS = 30000;

/** A stored goodbye reason older than this is treated as stale and ignored, so
 *  an ancient goodbye can never be mistaken for the reason behind a much later,
 *  unclean disconnect. The poller consumes it within one cycle (~2s). */
const GOODBYE_TTL_MS = 10000;

export class SendspinCore {
  private readonly sessionsBySocket = new Map<WebSocket, SendspinSession>();
  private readonly hooksByClientId = new Map<
    string,
    { hooks: SendspinSessionHooks; context?: SendspinConnectionMeta }
  >();
  private readonly leadStatsByClientId = new Map<
    string,
    { leadUs: number; targetLeadUs: number; bufferedBytes?: number; updatedAt: number }
  >();
  // Liveness flag per socket; flipped false before each ping, true on pong.
  private readonly aliveBySocket = new Map<WebSocket, boolean>();
  // Last goodbye reason per clientId, captured at close so callers can tell an
  // intentional leave (user_request/shutdown) from a dropped socket afterwards.
  private readonly recentGoodbyeByClientId = new Map<string, { reason: GoodbyeReason; at: number }>();
  private readonly heartbeatTimer: ReturnType<typeof setInterval>;

  constructor() {
    this.heartbeatTimer = setInterval(() => this.sweepHeartbeats(), HEARTBEAT_INTERVAL_MS);
    // Don't keep the process alive just for the heartbeat.
    this.heartbeatTimer.unref?.();
  }

  /** Terminate sockets that didn't pong since the last sweep; ping the rest.
   *  `terminate()` fires `'close'`, which runs the normal session cleanup. */
  private sweepHeartbeats(): void {
    for (const ws of this.sessionsBySocket.keys()) {
      if (this.aliveBySocket.get(ws) === false) {
        ws.terminate();
        continue;
      }
      this.aliveBySocket.set(ws, false);
      try {
        ws.ping();
      } catch {
        // Socket already closing; the next sweep (or 'close') cleans it up.
      }
    }
  }

  handleConnection(
    ws: WebSocket,
    req?: IncomingMessage | null,
    connectionReason: ConnectionReason = ConnectionReason.DISCOVERY,
  ): void {
    const meta = this.extractConnectionMetadata(req);
    const session = new SendspinSession(ws, req ?? null, connectionReason, {
      zoneId: meta.zoneId,
      playerId: meta.playerId,
      remote: req?.socket?.remoteAddress ?? null,
    });
    this.sessionsBySocket.set(ws, session);
    this.aliveBySocket.set(ws, true);

    ws.on('pong', () => {
      this.aliveBySocket.set(ws, true);
    });

    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        session.handleBinary(data);
      } else {
        session.handleText(data.toString());
        const info = session.getClientId();
        if (info && !session.hasHooksAttached()) {
          const entry = this.hooksByClientId.get(info);
          if (entry) {
            session.setHooks(entry.hooks, entry.context);
          }
        }
      }
    });

    ws.on('close', () => {
      this.sessionsBySocket.delete(ws);
      this.aliveBySocket.delete(ws);
      const clientId = session.getClientId();
      const reason = session.getLastGoodbyeReason();
      if (clientId && reason) {
        this.recentGoodbyeByClientId.set(clientId, { reason, at: Date.now() });
      }
      session.destroy();
    });

    ws.on('error', () => {
      // Ignore; close will clean up.
    });
  }

  registerHooks(
    clientId: string,
    hooks: SendspinSessionHooks,
    context?: SendspinConnectionMeta,
  ): void {
    this.hooksByClientId.set(clientId, { hooks, context });
    const session = this.getSession(clientId);
    if (session) {
      session.setHooks(hooks, context);
    }
  }

  unregisterHooks(clientId: string): void {
    this.hooksByClientId.delete(clientId);
    const session = this.getSession(clientId);
    if (session) {
      session.setHooks({});
    }
  }

  listClients(): Array<{
    clientId: string | null;
    name: string | null;
    roles: RoleName[];
    playbackState: PlaybackStateType;
    remote: string | null;
    sourceState: SourceStateType | null;
    sourceSignal: SourceSignalType | null;
  }> {
    const items: Array<{
      clientId: string | null;
      name: string | null;
      roles: RoleName[];
      playbackState: PlaybackStateType;
      remote: string | null;
      sourceState: SourceStateType | null;
      sourceSignal: SourceSignalType | null;
    }> = [];
    for (const session of this.sessionsBySocket.values()) {
      const sourceStatus = session.getSourceStatus();
      items.push({
        clientId: session.getClientId(),
        name: session.getClientName(),
        roles: session.getRoles(),
        remote: session.getRemoteAddress(),
        playbackState: session.getDescriptor().playbackState,
        sourceState: sourceStatus.state ?? null,
        sourceSignal: sourceStatus.signal ?? null,
      });
    }
    return items;
  }

  listDescriptors(): Array<{
    clientId: string | null;
    roles: RoleName[];
    playbackState: 'playing' | 'paused' | 'stopped';
    remote: string | null;
  }> {
    const items: Array<{
      clientId: string | null;
      roles: RoleName[];
      playbackState: 'playing' | 'paused' | 'stopped';
      remote: string | null;
    }> = [];
    for (const session of this.sessionsBySocket.values()) {
      items.push(session.getDescriptor());
    }
    return items;
  }

  getSessionBySocket(ws: WebSocket): SendspinSession | undefined {
    return this.sessionsBySocket.get(ws);
  }

  getSessions(): Iterable<SendspinSession> {
    return this.sessionsBySocket.values();
  }

  /** Read and clear the goodbye reason a client reported as it last left.
   *  Returns null if it dropped without a goodbye (unclean disconnect) or the
   *  recorded reason is stale. Lets callers skip the reconnect grace for an
   *  intentional `user_request`/`shutdown` leave. */
  takeGoodbyeReason(clientId: string): GoodbyeReason | null {
    const entry = this.recentGoodbyeByClientId.get(clientId);
    if (!entry) return null;
    this.recentGoodbyeByClientId.delete(clientId);
    return Date.now() - entry.at <= GOODBYE_TTL_MS ? entry.reason : null;
  }

  sendPcmFrameToClient(clientId: string, frame: SendspinPcmFrame): void {
    this.getSession(clientId)?.sendPcmAudioFrame(frame);
  }

  sendStreamStart(clientId: string, format?: Partial<PlayerFormat>): void {
    this.getSession(clientId)?.sendStreamStart(format);
  }

  sendStreamClear(clientId: string, roles?: RoleName[]): void {
    this.getSession(clientId)?.sendStreamClear(roles);
  }

  sendStreamEnd(clientId: string, roles?: RoleName[]): void {
    this.getSession(clientId)?.sendStreamEnd(roles);
  }

  sendGroupUpdate(
    clientId: string,
    playbackState: PlaybackStateType,
    groupId?: string,
    groupName?: string,
  ): void {
    this.getSession(clientId)?.sendGroupUpdate(playbackState, groupId, groupName);
  }

  sendMetadata(clientId: string, payload: Parameters<SendspinSession['sendMetadata']>[0]): void {
    this.getSession(clientId)?.sendMetadata(payload);
  }

  setClientMetadata(clientId: string, payload: Parameters<SendspinSession['sendMetadata']>[0]): void {
    this.sendMetadata(clientId, payload);
  }

  sendServerCommand(
    clientId: string,
    payload: Parameters<SendspinSession['sendServerCommand']>[0] | ServerCommandPayload,
  ): void {
    this.getSession(clientId)?.sendServerCommand(payload as ServerCommandPayload);
  }

  sendArtworkStreamStart(clientId: string, channels: Parameters<SendspinSession['sendArtworkStreamStart']>[0]): void {
    this.getSession(clientId)?.sendArtworkStreamStart(channels);
  }

  sendArtwork(clientId: string, channel: 0 | 1 | 2 | 3, imageData: Buffer | null): void {
    this.getSession(clientId)?.sendArtwork(channel, imageData);
  }

  sendVisualizerStreamStart(clientId: string, config?: Record<string, any>): void {
    this.getSession(clientId)?.sendVisualizerStreamStart(config);
  }

  sendVisualizerFrame(clientId: string, data: Buffer, timestampUs?: number): void {
    this.getSession(clientId)?.sendVisualizerFrame(data, timestampUs);
  }

  sendControllerState(clientId: string, payload: Parameters<SendspinSession['sendControllerState']>[0]): void {
    this.getSession(clientId)?.sendControllerState(payload);
  }

  sendColor(clientId: string, payload: Parameters<SendspinSession['sendColor']>[0]): void {
    this.getSession(clientId)?.sendColor(payload);
  }

  setClientControllerState(clientId: string, payload: Parameters<SendspinSession['sendControllerState']>[0]): void {
    this.sendControllerState(clientId, payload);
  }

  setClientPlaybackState(
    clientId: string,
    playbackState: PlaybackStateType,
    groupId?: string,
    groupName?: string,
  ): void {
    this.sendGroupUpdate(clientId, playbackState, groupId, groupName);
  }

  getSessionByClientId(clientId: string): SendspinSession | undefined {
    return this.getSession(clientId);
  }

  getStreamFormat(clientId: string): ReturnType<SendspinSession['getStreamFormat']> | null {
    return this.getSession(clientId)?.getStreamFormat() ?? null;
  }

  getPlayerBufferCapacity(clientId: string): number | null {
    const cap = this.getSession(clientId)?.getPlayerBufferCapacity() ?? 0;
    return cap > 0 ? cap : null;
  }

  setLeadStats(
    clientId: string,
    stats: { leadUs: number; targetLeadUs: number; bufferedBytes?: number },
  ): void {
    if (!clientId) return;
    this.leadStatsByClientId.set(clientId, {
      leadUs: stats.leadUs,
      targetLeadUs: stats.targetLeadUs,
      bufferedBytes: stats.bufferedBytes,
      updatedAt: Date.now(),
    });
  }

  clearLeadStats(clientId: string): void {
    if (!clientId) return;
    this.leadStatsByClientId.delete(clientId);
  }

  getLeadStats(
    clientId: string,
  ): { leadUs: number; targetLeadUs: number; bufferedBytes?: number; updatedAt: number } | null {
    return this.leadStatsByClientId.get(clientId) ?? null;
  }

  getArtworkChannels(
    clientId: string,
  ): ReturnType<SendspinSession['getArtworkChannels']> | null {
    return this.getSession(clientId)?.getArtworkChannels() ?? null;
  }

  getBackpressureStats(clientId: string): ReturnType<SendspinSession['getBackpressureStats']> | null {
    return this.getSession(clientId)?.getBackpressureStats() ?? null;
  }

  private getSession(clientId: string): SendspinSession | undefined {
    let preferred: SendspinSession | undefined;
    let fallback: SendspinSession | undefined;
    for (const session of this.sessionsBySocket.values()) {
      if (session.getClientId() === clientId) {
        if (session.getConnectionReason() === ConnectionReason.PLAYBACK && !preferred) {
          preferred = session;
        } else if (!fallback) {
          fallback = session;
        }
      }
    }
    return preferred ?? fallback;
  }

  private extractConnectionMetadata(
    req?: IncomingMessage | null,
  ): {
    zoneId?: number;
    playerId?: string;
  } {
    if (!req?.url) {
      return {};
    }
    try {
      const url = new URL(req.url, 'http://localhost');
      const zoneStr = url.searchParams.get('zone');
      const zoneId = zoneStr && Number.isFinite(Number(zoneStr)) ? Number(zoneStr) : undefined;
      const playerId = url.searchParams.get('player') ?? undefined;
      return { zoneId, playerId };
    } catch {
      return {};
    }
  }
}
