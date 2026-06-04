import type { IncomingMessage } from 'node:http';
import WebSocket from 'ws';

import { BINARY_HEADER_SIZE, packBinaryHeaderRaw, unpackBinaryHeader } from '../binary.js';
import {
  AudioCodec,
  BinaryMessageType,
  ClientCommandMessage,
  ClientInboundMessage,
  ClientGoodbyeMessage,
  ClientStateMessage,
  ClientStateType,
  ClientHelloSourceSupport,
  SourceControl,
  SourceFormat,
  SourceStatePayload,
  SourceStateType,
  SourceSignalType,
  ClientTimeMessage,
  ConnectionReason,
  ControllerStatePayload,
  GoodbyeReason,
  MediaCommand,
  PlaybackStateType,
  PlayerCommand,
  RepeatMode,
  RoleName,
  Roles,
  ServerCommandMessage,
  ServerCommandPayload,
  ServerHelloMessage,
  ServerHelloPayload,
  ServerStateMessage,
  ServerTimeMessage,
  StreamClearMessage,
  StreamEndMessage,
  StreamRequestFormatMessage,
  StreamRequestFormatPayload,
  StreamStartMessage,
  StreamStartPayload,
  StreamStartPlayer,
  SourceClientCommand,
} from '../types.js';
import { serverNowUs } from './clock.js';

export type SendspinConnectionMeta = {
  zoneId?: number;
  playerId?: string;
  remote?: string | null;
};

export interface SendspinPcmFrame {
  data: Buffer;
  timestampUs?: number;
}

export interface PlayerFormat {
  codec: AudioCodec;
  sampleRate: number;
  channels: number;
  bitDepth: number;
  codecHeader?: string;
}

export type PlayerFormatWithBitDepth<T extends number = number> =
  Omit<PlayerFormat, 'bitDepth'> & { bitDepth: T };

export interface SendspinPlayerStateUpdate {
  clientId: string | null;
  roles: RoleName[];
  state?: ClientStateType;
  volume?: number;
  muted?: boolean;
}

export interface SendspinGroupCommand {
  clientId: string | null;
  roles: RoleName[];
  command: MediaCommand;
  volume?: number;
  mute?: boolean;
}

export interface SendspinSourceStateUpdate {
  clientId: string | null;
  roles: RoleName[];
  state?: SourceStateType;
  level?: number;
  signal?: SourceSignalType;
}

export interface SendspinSourceCommand {
  clientId: string | null;
  roles: RoleName[];
  command: SourceClientCommand;
}

export interface SendspinSourceAudioChunk {
  timestampUs: number;
  data: Buffer;
}

export interface SendspinSessionHooks {
  onPlayerState?: (session: SendspinSession, update: SendspinPlayerStateUpdate) => void;
  onGroupCommand?: (session: SendspinSession, command: SendspinGroupCommand) => void;
  onSourceState?: (session: SendspinSession, update: SendspinSourceStateUpdate) => void;
  onSourceCommand?: (session: SendspinSession, command: SendspinSourceCommand) => void;
  onSourceAudio?: (session: SendspinSession, chunk: SendspinSourceAudioChunk) => void;
  onIdentified?: (session: SendspinSession, req: IncomingMessage | null) => void;
  onDisconnected?: (session: SendspinSession) => void;
  onFormatChanged?: (session: SendspinSession, format: PlayerFormat) => void;
  onGoodbye?: (session: SendspinSession, reason: GoodbyeReason) => void;
  onUnsupportedRoles?: (session: SendspinSession, roles: RoleName[]) => void;
}

/**
 * Per-connection Sendspin session. Handles handshake, time sync, player/controller state,
 * and sending stream/metadata/commands to the client.
 */
export class SendspinSession {
  private ready = false;
  private clientId: string | null = null;
  private clientName: string | null = null;
  private roles: RoleName[] = [];
  private playerSupport: any = null;
  private sourceSupport: ClientHelloSourceSupport | null = null;
  private artworkChannels: Array<{
    source: 'album' | 'artist' | 'none';
    format: 'jpeg' | 'png' | 'bmp';
    width: number;
    height: number;
  }> = [];
  private expectVolume = false;
  private expectMute = false;
  private warnedMissingVolume = false;
  private warnedMissingMute = false;
  private initialStateReceived = false;
  private initialStateTimer: NodeJS.Timeout | null = null;
  private initialStateRequired = false;
  private identified = false;
  private playbackState: PlaybackStateType = PlaybackStateType.STOPPED;
  private lastStateSignature: string | null = null;
  private sourceState: SourceStateType | null = null;
  private sourceSignal: SourceSignalType | null = null;

  private hooks: SendspinSessionHooks = {};
  private hooksAttached = false;
  private activeStream = false;
  private streamFormat: PlayerFormat = {
    codec: AudioCodec.PCM,
    sampleRate: 48000,
    channels: 2,
    bitDepth: 16,
  };
  private lastGoodbyeReason: GoodbyeReason | null = null;

  private readonly maxBufferedSend = 1024 * 512; // ~512KB backpressure guard
  private backpressureDrops = 0;
  private lastBackpressureBytes = 0;
  private lastBackpressureTs = 0;
  private backpressureEvents: number[] = [];

  constructor(
    private readonly ws: WebSocket,
    private readonly req: IncomingMessage | null,
    private connectionReason: ConnectionReason = ConnectionReason.DISCOVERY,
    private readonly connectionMeta: SendspinConnectionMeta = {},
    hooks: SendspinSessionHooks = {},
  ) {
    this.hooks = hooks;
  }

  setHooks(
    hooks: SendspinSessionHooks,
    context?: SendspinConnectionMeta,
  ): void {
    this.hooks = hooks;
    this.hooksAttached = true;
    if (context) Object.assign(this.connectionMeta, context);
    this.maybeNotifyIdentified();
  }

  hasHooksAttached(): boolean {
    return this.hooksAttached;
  }

  getClientId(): string | null {
    return this.clientId;
  }

  getClientName(): string | null {
    return this.clientName;
  }

  getRoles(): RoleName[] {
    return this.roles;
  }

  getInfo(): { id: string | null; roles: RoleName[]; playbackState: PlaybackStateType } {
    return {
      id: this.clientId,
      roles: [...this.roles],
      playbackState: this.playbackState,
    };
  }

  getDescriptor(): {
    clientId: string | null;
    roles: RoleName[];
    playbackState: PlaybackStateType;
    remote: string | null;
  } {
    return {
      clientId: this.clientId,
      roles: [...this.roles],
      playbackState: this.playbackState,
      remote: this.getRemoteAddress(),
    };
  }

  getConnectionReason(): ConnectionReason {
    return this.connectionReason;
  }

  getLastGoodbyeReason(): GoodbyeReason | null {
    return this.lastGoodbyeReason;
  }

  getRemoteAddress(): string | null {
    return this.connectionMeta.remote ?? this.req?.socket?.remoteAddress ?? null;
  }

  getStreamFormat(): PlayerFormat {
    return { ...this.streamFormat };
  }

  getSourceStatus(): { state: SourceStateType | null; signal: SourceSignalType | null } {
    return { state: this.sourceState, signal: this.sourceSignal };
  }

  getSourceSupport(): ClientHelloSourceSupport | null {
    if (!this.sourceSupport) {
      return null;
    }
    return {
      ...this.sourceSupport,
      supported_formats: Array.isArray(this.sourceSupport.supported_formats)
        ? this.sourceSupport.supported_formats.map((fmt) => ({ ...fmt }))
        : this.sourceSupport.supported_formats,
      features: this.sourceSupport.features ? { ...this.sourceSupport.features } : this.sourceSupport.features,
      controls: Array.isArray(this.sourceSupport.controls)
        ? [...this.sourceSupport.controls]
        : this.sourceSupport.controls,
    };
  }

  getBackpressureStats(): { drops: number; lastBytes: number; lastDropTs: number | null; recentDrops: number } {
    const cutoff = Date.now() - 5 * 60 * 1000;
    this.backpressureEvents = this.backpressureEvents.filter((ts) => ts >= cutoff);
    return {
      drops: this.backpressureDrops,
      lastBytes: this.lastBackpressureBytes,
      lastDropTs: this.lastBackpressureTs || null,
      recentDrops: this.backpressureEvents.length,
    };
  }

  getPlayerBufferCapacity(): number {
    const cap = this.playerSupport?.buffer_capacity;
    return typeof cap === 'number' && cap > 0 ? cap : 0;
  }

  getArtworkChannels(): SendspinSession['artworkChannels'] {
    return [...this.artworkChannels];
  }

  destroy(): void {
    if (this.initialStateTimer) {
      clearTimeout(this.initialStateTimer);
      this.initialStateTimer = null;
    }
    if (this.hooks.onDisconnected) {
      this.hooks.onDisconnected(this);
    }
  }

  handleText(text: string): void {
    let msg: ClientInboundMessage & { type: string };
    try {
      msg = JSON.parse(text);
    } catch {
      return;
    }
    if (!this.ready) {
      if (msg.type !== 'client/hello') {
        try {
          this.ws.close(1008, 'expected client/hello first');
        } catch {
          /* ignore */
        }
        return;
      }
      this.handleHello((msg as any).payload);
      return;
    }
    switch (msg.type) {
      case 'client/hello':
        break;
      case 'client/time':
        this.handleClientTime((msg as ClientTimeMessage).payload);
        break;
      case 'client/state':
        this.handleClientState((msg as ClientStateMessage).payload);
        break;
      case 'client/command':
        this.handleClientCommand((msg as ClientCommandMessage).payload);
        break;
      case 'client/goodbye':
        this.handleClientGoodbye((msg as ClientGoodbyeMessage).payload);
        break;
      case 'stream/request-format':
        this.handleStreamRequestFormat((msg as StreamRequestFormatMessage).payload);
        break;
      default:
        break;
    }
  }

  handleBinary(data: WebSocket.RawData): void {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
    let header: { messageType: number; timestampUs: number } | null = null;
    try {
      header = unpackBinaryHeader(buf);
    } catch {
      return;
    }
    if (!header) return;
    if (header.messageType === BinaryMessageType.SOURCE_AUDIO_CHUNK) {
      if (!this.roles.includes(Roles.SOURCE)) {
        return;
      }
      const chunk = buf.subarray(BINARY_HEADER_SIZE);
      if (this.hooks.onSourceAudio) {
        this.hooks.onSourceAudio(this, { timestampUs: header.timestampUs, data: chunk });
      }
    }
  }

  private handleHello(payload: any): void {
    const version = typeof payload?.version === 'number' ? payload.version : null;
    if (version !== 1) {
      try {
        this.ws.close(1008, 'invalid protocol version');
      } catch {
        /* ignore */
      }
      return;
    }
    const rawClientId = typeof payload?.client_id === 'string' ? payload.client_id.trim() : '';
    if (!rawClientId) {
      try {
        this.ws.close(1008, 'missing client_id');
      } catch {
        /* ignore */
      }
      return;
    }
    this.clientId = rawClientId;
    this.clientName = typeof payload?.name === 'string' ? payload.name.trim() : null;
    const supportedRoles: RoleName[] = Array.isArray(payload.supported_roles) ? payload.supported_roles : [];
    if (!supportedRoles.length) {
      try {
        this.ws.close(1008, 'missing supported_roles');
      } catch {
        /* ignore */
      }
      return;
    }
    const { activeRoles, unsupportedRoles } = this.resolveActiveRoles(supportedRoles);
    this.roles = activeRoles;
    const playerSupport = payload['player@v1_support'] ?? payload.player_support ?? null;
    const artworkSupport = payload['artwork@v1_support'] ?? payload.artwork_support ?? null;
    const visualizerSupport =
      payload['visualizer@_draft_r1_support'] ??
      payload['visualizer@v1_support'] ??
      payload.visualizer_support ??
      null;
    const sourceSupport = payload['source@v1_support'] ?? payload.source_support ?? null;
    if (this.roles.includes(Roles.PLAYER) && !playerSupport) {
      try {
        this.ws.close(1008, 'missing player support');
      } catch {
        /* ignore */
      }
      return;
    }
    if (this.roles.includes(Roles.ARTWORK) && !artworkSupport) {
      try {
        this.ws.close(1008, 'missing artwork support');
      } catch {
        /* ignore */
      }
      return;
    }
    if (this.roles.includes(Roles.VISUALIZER) && !visualizerSupport) {
      try {
        this.ws.close(1008, 'missing visualizer support');
      } catch {
        /* ignore */
      }
      return;
    }
    if (this.roles.includes(Roles.SOURCE) && !sourceSupport) {
      try {
        this.ws.close(1008, 'missing source support');
      } catch {
        /* ignore */
      }
      return;
    }
    this.playerSupport = playerSupport || {};
    this.sourceSupport = this.normalizeSourceSupport(sourceSupport);
    if (this.roles.includes(Roles.SOURCE) && !this.sourceSupport) {
      try {
        this.ws.close(1008, 'invalid source support');
      } catch {
        /* ignore */
      }
      return;
    }
    this.expectVolume = (this.playerSupport?.supported_commands ?? []).includes(PlayerCommand.VOLUME);
    this.expectMute = (this.playerSupport?.supported_commands ?? []).includes(PlayerCommand.MUTE);
    const artworkChannels = Array.isArray(artworkSupport?.channels) ? artworkSupport.channels : [];
    if (artworkChannels.length) {
      this.artworkChannels = artworkChannels.map((c: any) => ({
        source: c?.source === 'artist' ? 'artist' : c?.source === 'none' ? 'none' : 'album',
        format: c?.format === 'png' ? 'png' : c?.format === 'bmp' ? 'bmp' : 'jpeg',
        width: typeof c?.media_width === 'number' ? c.media_width : 800,
        height: typeof c?.media_height === 'number' ? c.media_height : 800,
      }));
    }
    this.initialStateRequired = this.roles.includes(Roles.PLAYER);
    this.applyPreferredStreamFormat();
    this.ready = true;
    this.sendServerHello();
    this.startInitialStateTimeout();
    const defaultGroup = this.getDefaultGroupInfo();
    this.sendGroupUpdate(PlaybackStateType.STOPPED, defaultGroup.groupId, defaultGroup.groupName);
    if (unsupportedRoles.length && this.hooks.onUnsupportedRoles) {
      this.hooks.onUnsupportedRoles(this, unsupportedRoles);
    }
    this.maybeNotifyIdentified();
  }

  private handleClientTime(payload: ClientTimeMessage['payload']): void {
    const nowUs = serverNowUs();
    const message: ServerTimeMessage = {
      type: 'server/time',
      payload: {
        client_transmitted: payload.client_transmitted,
        server_received: nowUs,
        server_transmitted: serverNowUs(),
      },
    };
    this.sendJson(message);
  }

  private handleClientState(payload: ClientStateMessage['payload']): void {
    if (!this.initialStateReceived) {
      this.initialStateReceived = true;
      if (this.initialStateTimer) {
        clearTimeout(this.initialStateTimer);
        this.initialStateTimer = null;
      }
      this.maybeNotifyIdentified();
    }
    const roles = [...this.roles];
    const state =
      typeof payload.state === 'string'
        ? (payload.state as ClientStateType)
        : typeof payload.player?.state === 'string'
          ? (payload.player.state as ClientStateType)
          : undefined;
    const update: SendspinPlayerStateUpdate = {
      clientId: this.clientId,
      roles,
      state,
      volume: payload.player?.volume,
      muted: payload.player?.muted,
    };
    if (this.expectVolume && update.volume === undefined && !this.warnedMissingVolume) {
      this.warnedMissingVolume = true;
    }
    if (this.expectMute && update.muted === undefined && !this.warnedMissingMute) {
      this.warnedMissingMute = true;
    }
    if (this.hooks.onPlayerState) {
      this.hooks.onPlayerState(this, update);
    }
    if (payload.source && this.hooks.onSourceState) {
      this.sourceState = payload.source.state ?? null;
      this.sourceSignal = payload.source.signal ?? null;
      this.hooks.onSourceState(this, {
        clientId: this.clientId,
        roles,
        state: payload.source.state,
        level: payload.source.level,
        signal: payload.source.signal,
      });
    } else if (payload.source) {
      this.sourceState = payload.source.state ?? null;
      this.sourceSignal = payload.source.signal ?? null;
    }
  }

  private handleClientCommand(payload: {
    controller?: { command: MediaCommand; volume?: number; mute?: boolean };
    source?: { command: SourceClientCommand };
  }): void {
    const roles = [...this.roles];
    const controller = payload.controller;
    if (controller) {
      const cmd: SendspinGroupCommand = {
        clientId: this.clientId,
        roles,
        command: controller.command,
        volume: controller.volume,
        mute: controller.mute,
      };
      this.hooks.onGroupCommand?.(this, cmd);
    }
    const source = payload.source;
    if (source && this.hooks.onSourceCommand) {
      this.hooks.onSourceCommand(this, {
        clientId: this.clientId,
        roles,
        command: source.command,
      });
    }
  }

  private handleClientGoodbye(payload: ClientGoodbyeMessage['payload']): void {
    const reason = payload?.reason;
    if (reason) {
      this.lastGoodbyeReason = reason;
      this.hooks.onGoodbye?.(this, reason);
    }
    try {
      this.ws.close();
    } catch {
      /* ignore */
    }
  }

  private handleStreamRequestFormat(payload: StreamRequestFormatPayload): void {
    if (payload.player) {
      if (!this.roles.includes(Roles.PLAYER)) {
        return;
      }
      this.applyPlayerFormatRequest(payload.player);
    }
    if (payload.artwork) {
      if (!this.roles.includes(Roles.ARTWORK)) {
        return;
      }
      this.applyArtworkFormatRequest(payload.artwork);
    }
    this.hooks.onFormatChanged?.(this, this.streamFormat);
    this.sendStreamFormat();
  }

  private sendServerHello(): void {
    if (!this.roles.length) return;
    const payload: ServerHelloPayload = {
      server_id: 'server',
      name: 'Sendspin Server',
      version: 1,
      active_roles: this.roles.map((r) => r as any),
      connection_reason: this.connectionReason,
    };
    const message: ServerHelloMessage = { type: 'server/hello', payload };
    this.sendJson(message);
  }

  sendStreamStart(format?: Partial<PlayerFormat>): void {
    if (!this.ready) return;
    const merged: PlayerFormat = {
      ...this.streamFormat,
      ...format,
    };
    this.streamFormat = merged;
    const player: StreamStartPlayer = {
      codec: merged.codec,
      sample_rate: merged.sampleRate,
      channels: merged.channels,
      bit_depth: merged.bitDepth,
      codec_header: merged.codecHeader,
    };
    const payload: StreamStartPayload = { player };
    const message: StreamStartMessage = { type: 'stream/start', payload };
    this.activeStream = true;
    this.sendJson(message);
  }

  private sendStreamFormat(): void {
    if (!this.ready) return;
    const merged = this.streamFormat;
    const player: StreamStartPlayer = {
      codec: merged.codec,
      sample_rate: merged.sampleRate,
      channels: merged.channels,
      bit_depth: merged.bitDepth,
      codec_header: merged.codecHeader,
    };
    const payload: StreamStartPayload = { player };
    const message: StreamStartMessage = { type: 'stream/start', payload };
    this.activeStream = true;
    this.sendJson(message);
  }

  sendStreamClear(roles?: RoleName[]): void {
    if (!this.ready) return;
    // Per Sendspin spec, stream/clear only applies to roles that maintain
    // client-side buffers: player and visualizer. Drop other entries rather
    // than forwarding an invalid payload that the client would reject.
    const filtered = roles?.filter((role) => {
      const family = (typeof role === 'string' ? role : '').split('@')[0];
      return family === 'player' || family === 'visualizer';
    });
    const payload = { roles: filtered as any };
    const message: StreamClearMessage = { type: 'stream/clear', payload };
    this.sendJson(message);
  }

  sendStreamEnd(roles?: RoleName[]): void {
    if (!this.ready) return;
    const payload = { roles: roles as any };
    const message: StreamEndMessage = { type: 'stream/end', payload };
    this.activeStream = false;
    this.sendJson(message);
  }

  sendPcmAudioFrame(frame: SendspinPcmFrame): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.ensureStreamStarted();
    const buffered = this.ws.bufferedAmount;
    if (buffered > this.maxBufferedSend) {
      setTimeout(() => {
        if (this.ws.readyState === WebSocket.OPEN) {
          const ts = frame.timestampUs ?? serverNowUs();
          const header = packBinaryHeaderRaw(BinaryMessageType.AUDIO_CHUNK, ts);
          this.sendBinary(Buffer.concat([header, frame.data]));
        }
      }, 5);
      return;
    }
    const ts = frame.timestampUs ?? serverNowUs();
    const header = packBinaryHeaderRaw(BinaryMessageType.AUDIO_CHUNK, ts);
    this.sendBinary(Buffer.concat([header, frame.data]));
  }

  sendServerCommand(payload: ServerCommandPayload): void;
  sendServerCommand(
    command: PlayerCommand,
    payload?: { volume?: number; mute?: boolean; static_delay_ms?: number },
  ): void;
  sendServerCommand(
    payloadOrCommand: ServerCommandPayload | PlayerCommand,
    payload?: { volume?: number; mute?: boolean; static_delay_ms?: number },
  ): void {
    if (!this.ready) return;
    const commandPayload: ServerCommandPayload =
      typeof payloadOrCommand === 'string'
        ? {
            player: {
              command: payloadOrCommand,
              volume: payload?.volume,
              mute: payload?.mute,
              static_delay_ms: payload?.static_delay_ms,
            },
          }
        : payloadOrCommand;
    const wantsPlayer = !!commandPayload.player;
    const wantsSource = !!commandPayload.source;
    const canSendPlayer = wantsPlayer && this.roles.includes(Roles.PLAYER);
    const canSendSource = wantsSource && this.roles.includes(Roles.SOURCE);
    if (!canSendPlayer && !canSendSource) {
      return;
    }
    const message: ServerCommandMessage = { type: 'server/command', payload: commandPayload };
    this.sendJson(message);
  }

  sendGroupUpdate(playbackState: PlaybackStateType, groupId?: string, groupName?: string): void {
    if (!this.ready) return;
    this.playbackState = playbackState;
    const message = {
      type: 'group/update',
      payload: {
        playback_state: playbackState,
        group_id: groupId,
        group_name: groupName,
      },
    };
    this.sendJson(message as any);
  }

  sendMetadata(metadata: {
    title?: string | null;
    artist?: string | null;
    album_artist?: string | null;
    album?: string | null;
    artwork_url?: string | null;
    year?: number | null;
    track?: number | null;
    repeat?: RepeatMode | null;
    shuffle?: boolean | null;
    progress?: {
      track_progress: number;
      track_duration: number;
      playback_speed: number;
    } | null;
  }): void {
    if (!this.ready) return;
    if (!this.roles.includes(Roles.METADATA)) return;
    const message: ServerStateMessage = {
      type: 'server/state',
      payload: {
        metadata: {
          timestamp: serverNowUs(),
          ...metadata,
        },
      },
    };
    this.sendJson(message);
  }

  sendControllerState(controller: ControllerStatePayload): void {
    if (!this.ready) return;
    if (!this.roles.includes(Roles.CONTROLLER)) return;
    const message: ServerStateMessage = {
      type: 'server/state',
      payload: { controller },
    };
    this.sendJson(message);
  }

  sendArtworkStreamStart(
    channels: Array<{ source: 'album' | 'artist' | 'none'; format: 'jpeg' | 'png' | 'bmp'; width: number; height: number }>,
  ): void {
    if (!this.ready) return;
    if (!this.roles.includes(Roles.ARTWORK)) return;
    this.artworkChannels = channels;
    const message = {
      type: 'stream/start',
      payload: {
        artwork: {
          channels: channels.map((c) => ({
            source: c.source,
            format: c.format,
            width: c.width,
            height: c.height,
          })),
        },
      },
    };
    this.sendJson(message as any);
  }

  sendArtwork(channel: 0 | 1 | 2 | 3, imageData: Buffer | null): void {
    if (!this.ready) return;
    const header = packBinaryHeaderRaw(
      BinaryMessageType.ARTWORK_CHANNEL_0 + channel,
      serverNowUs(),
    );
    const payload = imageData ? Buffer.concat([header, imageData]) : header;
    this.sendBinary(payload);
  }

  sendVisualizerStreamStart(config: Record<string, any> = {}): void {
    if (!this.ready) return;
    if (!this.roles.includes(Roles.VISUALIZER)) return;
    const message = {
      type: 'stream/start',
      payload: { visualizer: { ...config } },
    };
    this.sendJson(message as any);
  }

  sendVisualizerFrame(data: Buffer, timestampUs?: number): void {
    if (!this.ready) return;
    if (!this.roles.includes(Roles.VISUALIZER)) return;
    const ts = timestampUs ?? serverNowUs();
    const header = packBinaryHeaderRaw(BinaryMessageType.VISUALIZATION_DATA, ts);
    this.sendBinary(Buffer.concat([header, data]));
  }

  private applyPlayerFormatRequest(request: StreamRequestFormatPayload['player']): void {
    if (!request) return;
    const codec = this.normalizeCodec(request.codec);
    const merged: PlayerFormat = {
      ...this.streamFormat,
      ...(codec ? { codec } : {}),
      ...(typeof request.sample_rate === 'number' ? { sampleRate: request.sample_rate } : {}),
      ...(typeof request.channels === 'number' ? { channels: request.channels } : {}),
      ...(typeof request.bit_depth === 'number' ? { bitDepth: request.bit_depth } : {}),
    };
    this.streamFormat = merged;
  }

  private applyArtworkFormatRequest(request: StreamRequestFormatPayload['artwork']): void {
    if (!request || typeof request.channel !== 'number') return;
    const idx = Math.max(0, Math.min(3, Math.floor(request.channel)));
    if (idx < 0 || idx >= this.artworkChannels.length) {
      return;
    }
    const current = this.artworkChannels[idx] ?? {
      source: 'album' as const,
      format: 'jpeg' as const,
      width: 800,
      height: 800,
    };
    const next = {
      source: request.source ?? current.source,
      format: request.format ?? current.format,
      width: typeof request.media_width === 'number' ? request.media_width : current.width,
      height: typeof request.media_height === 'number' ? request.media_height : current.height,
    };
    this.artworkChannels[idx] = next;
    this.sendArtworkStreamStart(this.artworkChannels);
  }

  private applyPreferredStreamFormat(): void {
    const supportedFormats: any[] = Array.isArray(this.playerSupport?.supported_formats)
      ? this.playerSupport.supported_formats
      : [];
    if (!supportedFormats.length) return;

    const isSupported = (fmt: any): boolean =>
      fmt?.codec === 'opus' || fmt?.codec === 'flac' || fmt?.codec === 'pcm';
    const hasValidNumbers = (fmt: any): boolean =>
      typeof fmt.sample_rate === 'number' && fmt.sample_rate > 0
      && typeof fmt.channels === 'number' && fmt.channels > 0
      && typeof fmt.bit_depth === 'number' && fmt.bit_depth > 0;
    const preferred = supportedFormats.find((fmt) => isSupported(fmt) && hasValidNumbers(fmt));
    if (!preferred) return;
    const codec =
      preferred.codec === 'opus' ? AudioCodec.OPUS : preferred.codec === 'flac' ? AudioCodec.FLAC : AudioCodec.PCM;
    this.streamFormat = {
      codec,
      sampleRate: preferred.sample_rate,
      channels: preferred.channels,
      bitDepth: preferred.bit_depth,
    };
  }

  private normalizeCodec(codec?: AudioCodec | string): AudioCodec | undefined {
    if (!codec) return undefined;
    if (codec === AudioCodec.OPUS || codec === 'opus') return AudioCodec.OPUS;
    if (codec === AudioCodec.FLAC || codec === 'flac') return AudioCodec.FLAC;
    if (codec === AudioCodec.PCM || codec === 'pcm') return AudioCodec.PCM;
    return undefined;
  }

  private normalizeSourceSupport(raw: unknown): ClientHelloSourceSupport | null {
    if (!raw || typeof raw !== 'object') {
      return null;
    }
    const sourceSupport = raw as {
      supported_formats?: SourceFormat[];
      controls?: SourceControl[];
      features?: { level?: boolean; line_sense?: boolean };
    };
    const supportedFormats = Array.isArray(sourceSupport.supported_formats)
      ? sourceSupport.supported_formats.filter((fmt): fmt is SourceFormat => Boolean(fmt))
      : [];
    if (!supportedFormats.length) {
      return null;
    }
    const normalized: ClientHelloSourceSupport = {
      supported_formats: supportedFormats,
      ...(Array.isArray(sourceSupport.controls) ? { controls: [...sourceSupport.controls] } : {}),
      ...(sourceSupport.features && typeof sourceSupport.features === 'object'
        ? { features: { ...sourceSupport.features } }
        : {}),
    };
    return normalized;
  }

  private resolveActiveRoles(
    supportedRoles: RoleName[],
  ): { activeRoles: RoleName[]; unsupportedRoles: RoleName[] } {
    const serverSupported = new Set<RoleName>([
      Roles.PLAYER,
      Roles.CONTROLLER,
      Roles.METADATA,
      Roles.ARTWORK,
      Roles.VISUALIZER,
      Roles.SOURCE,
    ]);
    const activeRoles: RoleName[] = [];
    const unsupportedRoles: RoleName[] = [];
    const seenFamilies = new Set<string>();
    for (const role of supportedRoles) {
      if (typeof role !== 'string') continue;
      const family = role.split('@')[0];
      if (serverSupported.has(role)) {
        if (!seenFamilies.has(family)) {
          activeRoles.push(role);
          seenFamilies.add(family);
        }
        continue;
      }
      if (!role.startsWith('_')) {
        unsupportedRoles.push(role);
      }
    }
    return { activeRoles, unsupportedRoles };
  }

  private ensureStreamStarted(): void {
    if (this.activeStream) return;
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.sendStreamFormat();
  }

  private startInitialStateTimeout(): void {
    if (!this.roles.includes(Roles.PLAYER)) {
      return;
    }
    if (this.initialStateReceived || this.initialStateTimer) {
      return;
    }
    this.initialStateTimer = setTimeout(() => {
      if (this.initialStateReceived) {
        return;
      }
      try {
        this.ws.close(1008, 'initial state timeout');
      } catch {
        /* ignore */
      }
    }, 5000);
  }

  private maybeNotifyIdentified(): void {
    if (this.identified) {
      return;
    }
    if (!this.ready || !this.hooks.onIdentified) {
      return;
    }
    if (this.initialStateRequired && !this.initialStateReceived) {
      return;
    }
    this.hooks.onIdentified(this, this.req);
    this.identified = true;
  }

  private getDefaultGroupInfo(): { groupId: string; groupName: string } {
    if (this.connectionMeta.playerId) {
      return {
        groupId: this.connectionMeta.playerId,
        groupName: this.connectionMeta.playerId,
      };
    }
    if (typeof this.connectionMeta.zoneId === 'number') {
      const id = `zone-${this.connectionMeta.zoneId}`;
      return { groupId: id, groupName: id };
    }
    const fallback = this.clientId ?? 'sendspin';
    return { groupId: fallback, groupName: fallback };
  }

  private sendBinary(buf: Buffer, options: { allowDrop?: boolean } = {}): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    const buffered = this.ws.bufferedAmount;
    if (options.allowDrop === true && buffered > this.maxBufferedSend) {
      this.backpressureDrops += 1;
      this.lastBackpressureBytes = buffered;
      this.lastBackpressureTs = Date.now();
      this.backpressureEvents.push(this.lastBackpressureTs);
      return;
    }
    this.ws.send(buf);
  }

  private sendJson(message: any): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify(message));
  }
}
