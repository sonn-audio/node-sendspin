import EventEmitter from 'node:events';
import { performance } from 'node:perf_hooks';
import WebSocket from 'ws';

import {
  AudioCodec,
  ClientCommandPayload,
  ClientHelloMessage,
  ClientHelloPayload,
  ClientInboundMessage,
  ClientGoodbyeMessage,
  ClientStateMessage,
  ClientStatePayload,
  ClientTimePayload,
  ClientTimeMessage,
  GroupUpdateServerMessage,
  GroupUpdateServerPayload,
  PlaybackStateType,
  RoleName,
  Roles,
  ServerCommandMessage,
  ServerCommandPayload,
  ServerHelloMessage,
  ServerHelloPayload,
  ServerStateMessage,
  ServerStatePayload,
  ServerTimeMessage,
  ServerTimePayload,
  StreamClearMessage,
  StreamClearPayload,
  StreamEndMessage,
  StreamEndPayload,
  StreamStartMessage,
  StreamStartPayload,
  StreamStartPlayer,
  BinaryMessageType,
  ConnectionReason,
} from '../types.js';
import { packBinaryHeaderRaw } from '../binary.js';

export type ClientConnectionSide = 'incoming' | 'outgoing';

export interface ServerClientOptions {
  connectionSide: ClientConnectionSide;
  defaultRoles?: RoleName[];
  serverId: string;
  serverName: string;
  serverVersion?: number;
  connectionReason?: ConnectionReason;
}

export interface ClientEventMap {
  hello: (payload: ClientHelloPayload) => void;
  state: (payload: ClientStatePayload) => void;
  command: (payload: ClientCommandPayload) => void;
  disconnect: () => void;
}

export class ServerClient extends EventEmitter {
  private helloPayload?: ClientHelloPayload;
  private readonly ws: WebSocket;
  private readonly opts: ServerClientOptions;
  private readonly defaultRoles: RoleName[];
  private activeRoles: RoleName[] = [];
  private ready = false;
  private initialStateRequired = false;
  private initialStateReceived = false;
  private initialStateTimer: NodeJS.Timeout | null = null;
  private identified = false;
  private readonly maxBufferedSend = 1024 * 512;

  constructor(ws: WebSocket, opts: ServerClientOptions) {
    super();
    this.ws = ws;
    this.opts = opts;
    this.defaultRoles = opts.defaultRoles ?? [];
    this.setup();
  }

  get clientId(): string | undefined {
    return this.helloPayload?.client_id;
  }

  get name(): string | undefined {
    return this.helloPayload?.name;
  }

  get roles(): RoleName[] {
    return this.activeRoles.length ? this.activeRoles : this.helloPayload?.supported_roles ?? this.defaultRoles;
  }

  close(): void {
    if (this.ws.readyState === WebSocket.CLOSED || this.ws.readyState === WebSocket.CLOSING) {
      return;
    }
    this.ws.close();
  }

  async sendHello(): Promise<void> {
    const payload: ServerHelloPayload = {
      server_id: this.opts.serverId,
      name: this.opts.serverName,
      version: this.opts.serverVersion ?? 1,
      active_roles: this.roles.length ? this.roles : this.defaultRoles,
      connection_reason: this.opts.connectionReason ?? ConnectionReason.DISCOVERY,
    };
    const msg: ServerHelloMessage = { type: 'server/hello', payload };
    this.sendJson(msg);
  }

  sendGroupUpdate(payload: GroupUpdateServerPayload): void {
    const msg: GroupUpdateServerMessage = { type: 'group/update', payload };
    this.sendJson(msg);
  }

  sendServerState(payload: ServerStatePayload): void {
    const msg: ServerStateMessage = { type: 'server/state', payload };
    this.sendJson(msg);
  }

  sendServerCommand(payload: ServerCommandPayload): void {
    const msg: ServerCommandMessage = { type: 'server/command', payload };
    this.sendJson(msg);
  }

  sendStreamStart(payload: StreamStartPayload): void {
    const msg: StreamStartMessage = { type: 'stream/start', payload };
    this.sendJson(msg);
  }

  sendStreamClear(payload: StreamClearPayload): void {
    const msg: StreamClearMessage = { type: 'stream/clear', payload };
    this.sendJson(msg);
  }

  sendStreamEnd(payload: StreamEndPayload): void {
    const msg: StreamEndMessage = { type: 'stream/end', payload };
    this.sendJson(msg);
  }

  sendAudioChunk(timestampUs: number, pcmData: Buffer, player?: StreamStartPlayer): void {
    if (player && player.codec !== AudioCodec.PCM) {
      throw new Error('Only PCM chunks are supported in this implementation');
    }
    const header = packBinaryHeaderRaw(BinaryMessageType.AUDIO_CHUNK, timestampUs);
    const payload = Buffer.concat([header, pcmData]);
    if (this.ws.readyState !== WebSocket.OPEN) return;
    if (this.ws.bufferedAmount > this.maxBufferedSend) {
      setTimeout(() => {
        if (this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(payload);
        }
      }, 5);
      return;
    }
    this.ws.send(payload);
  }

  private setup(): void {
    this.ws.on('message', (data) => this.handleMessage(data));
    this.ws.on('close', () => this.emit('disconnect'));
    this.ws.on('error', () => this.emit('disconnect'));
  }

  private handleMessage(data: WebSocket.RawData): void {
    if (typeof data === 'string') {
      this.handleJson(data);
    } else if (data instanceof Buffer || data instanceof Uint8Array) {
      // Server currently only cares about binary audio from server to client; ignore inbound binary
    }
  }

  private handleJson(raw: string): void {
    let msg: ClientInboundMessage & { type: string };
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (!this.ready && msg.type !== 'client/hello') {
      this.closeWithReason('expected client/hello first');
      return;
    }

    switch (msg.type) {
      case 'client/hello': {
        if (this.ready) {
          break;
        }
        this.handleClientHello((msg as ClientHelloMessage).payload);
        break;
      }
      case 'client/time': {
        const payload = (msg as ClientTimeMessage).payload;
        this.respondTime(payload);
        break;
      }
      case 'client/state': {
        const payload = (msg as ClientStateMessage).payload;
        if (!this.initialStateReceived && this.initialStateRequired) {
          this.initialStateReceived = true;
          if (this.initialStateTimer) {
            clearTimeout(this.initialStateTimer);
            this.initialStateTimer = null;
          }
          this.maybeEmitHello();
        }
        this.emit('state', payload);
        break;
      }
      case 'client/command': {
        const payload = (msg as { payload: ClientCommandPayload }).payload;
        this.emit('command', payload);
        break;
      }
      case 'client/goodbye': {
        const payload = (msg as ClientGoodbyeMessage).payload;
        const reason = payload?.reason ? `client goodbye:${payload.reason}` : 'client goodbye';
        this.closeWithReason(reason);
        break;
      }
      default:
        break;
    }
  }

  private respondTime(payload: ClientTimePayload): void {
    const nowUs = this.nowUs();
    const response: ServerTimeMessage = {
      type: 'server/time',
      payload: {
        client_transmitted: payload.client_transmitted,
        server_received: nowUs,
        server_transmitted: this.nowUs(),
      },
    };
    this.sendJson(response);
  }

  private sendJson(message: ServerOutboundMessage): void {
    if (this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(JSON.stringify(message));
  }

  private nowUs(): number {
    return Math.floor(performance.now() * 1000);
  }

  private handleClientHello(payload: ClientHelloPayload): void {
    if (payload?.version !== 1) {
      this.closeWithReason('invalid protocol version');
      return;
    }
    const rawClientId = typeof payload?.client_id === 'string' ? payload.client_id.trim() : '';
    if (!rawClientId) {
      this.closeWithReason('missing client_id');
      return;
    }
    const supportedRoles = Array.isArray(payload?.supported_roles) ? payload.supported_roles : [];
    if (!supportedRoles.length) {
      this.closeWithReason('missing supported_roles');
      return;
    }

    const { activeRoles } = this.resolveActiveRoles(supportedRoles);
    this.activeRoles = activeRoles;

    const playerSupport = (payload as any)['player@v1_support'] ?? (payload as any).player_support ?? null;
    const artworkSupport = (payload as any)['artwork@v1_support'] ?? (payload as any).artwork_support ?? null;
    const visualizerSupport =
      (payload as any)['visualizer@_draft_r1_support'] ??
      (payload as any)['visualizer@v1_support'] ??
      (payload as any).visualizer_support ??
      null;
    const sourceSupport = (payload as any)['source@v1_support'] ?? (payload as any).source_support ?? null;
    if (this.activeRoles.includes(Roles.PLAYER) && !playerSupport) {
      this.closeWithReason('missing player support');
      return;
    }
    if (this.activeRoles.includes(Roles.ARTWORK) && !artworkSupport) {
      this.closeWithReason('missing artwork support');
      return;
    }
    if (this.activeRoles.includes(Roles.VISUALIZER) && !visualizerSupport) {
      this.closeWithReason('missing visualizer support');
      return;
    }
    if (this.activeRoles.includes(Roles.SOURCE) && !sourceSupport) {
      this.closeWithReason('missing source support');
      return;
    }

    this.helloPayload = payload;
    this.ready = true;
    void this.sendHello();
    this.sendGroupUpdate({
      playback_state: PlaybackStateType.STOPPED,
      group_id: rawClientId,
      group_name: rawClientId,
    });
    this.initialStateRequired = this.activeRoles.includes(Roles.PLAYER);
    if (this.initialStateRequired) {
      this.initialStateTimer = setTimeout(() => {
        if (!this.initialStateReceived) {
          this.closeWithReason('initial state timeout');
        }
      }, 5000);
    } else {
      this.maybeEmitHello();
    }
  }

  private maybeEmitHello(): void {
    if (this.identified || !this.helloPayload) return;
    if (this.initialStateRequired && !this.initialStateReceived) return;
    this.identified = true;
    this.emit('hello', this.helloPayload);
  }

  private closeWithReason(reason: string): void {
    if (this.initialStateTimer) {
      clearTimeout(this.initialStateTimer);
      this.initialStateTimer = null;
    }
    try {
      this.ws.close(1008, reason);
    } catch {
      /* ignore */
    }
  }

  private resolveActiveRoles(supportedRoles: RoleName[]): { activeRoles: RoleName[] } {
    const serverSupported = new Set<RoleName>([
      Roles.PLAYER,
      Roles.CONTROLLER,
      Roles.METADATA,
      Roles.ARTWORK,
      Roles.VISUALIZER,
      Roles.SOURCE,
    ]);
    const activeRoles: RoleName[] = [];
    const seenFamilies = new Set<string>();
    for (const role of supportedRoles) {
      if (typeof role !== 'string') continue;
      const family = role.split('@')[0];
      if (seenFamilies.has(family)) continue;
      if (serverSupported.has(role)) {
        activeRoles.push(role);
        seenFamilies.add(family);
      }
    }
    return { activeRoles };
  }
}

type ServerOutboundMessage =
  | ServerHelloMessage
  | ServerTimeMessage
  | GroupUpdateServerMessage
  | ServerStateMessage
  | StreamStartMessage
  | StreamClearMessage
  | StreamEndMessage
  | ServerCommandMessage;
