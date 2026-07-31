import { performance } from 'node:perf_hooks';
import WebSocket from 'ws';

import {
  AudioCodec,
  BinaryMessageType,
  ClientCommandMessage,
  ClientCommandPayload,
  ClientGoodbyeMessage,
  ClientHelloArtworkSupport,
  ClientHelloMessage,
  ClientHelloPayload,
  ClientHelloPlayerSupport,
  ClientHelloSourceSupport,
  ClientHelloVisualizerSupport,
  ClientOutboundMessage,
  ClientStateMessage,
  ClientStatePayload,
  ClientTimeMessage,
  ClientTimePayload,
  ClientStateType,
  ControllerCommandPayload,
  GroupUpdateServerMessage,
  GroupUpdateServerPayload,
  MediaCommand,
  PlayerStatePayload,
  RoleName,
  Roles,
  ServerCommandMessage,
  ServerCommandPayload,
  ServerHelloMessage,
  ServerHelloPayload,
  ServerInboundMessage,
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
  SourceClientCommand,
  SourceCommandPayload,
  SourceStatePayload,
} from './types.js';
import { BINARY_HEADER_SIZE, packBinaryHeaderRaw, unpackBinaryHeader } from './binary.js';
import { SendspinTimeFilter } from './time-filter.js';

export type MetadataCallback = (payload: ServerStatePayload) => void;
export type GroupUpdateCallback = (payload: GroupUpdateServerPayload) => void;
export type ControllerStateCallback = (payload: ServerStatePayload) => void;
export type StreamStartCallback = (message: StreamStartMessage) => void;
export type StreamEndCallback = (roles: RoleName[] | undefined) => void;
export type StreamClearCallback = (roles: RoleName[] | undefined) => void;
export type AudioChunkCallback = (timestampUs: number, audioData: Buffer, format: AudioFormat) => void;
export type DisconnectCallback = () => void;
export type ServerCommandCallback = (payload: ServerCommandPayload) => void;
export type SourceCommandCallback = (payload: SourceCommandPayload) => void;

export class PCMFormat {
  constructor(
    public readonly sampleRate: number,
    public readonly channels: number,
    public readonly bitDepth: number,
  ) {
    if (sampleRate <= 0) throw new Error('sampleRate must be positive');
    if (channels < 1 || channels > 2) throw new Error('channels must be 1 or 2');
    if (![16, 24, 32].includes(bitDepth)) throw new Error('bitDepth must be 16, 24, or 32');
  }

  get frameSize(): number {
    return this.channels * (this.bitDepth / 8);
  }
}

export class AudioFormat {
  constructor(
    public readonly codec: AudioCodec,
    public readonly pcm: PCMFormat,
    public readonly codecHeader?: Buffer,
  ) {}
}

export interface SendspinClientOptions {
  deviceInfo?: ClientHelloPayload['device_info'];
  playerSupport?: ClientHelloPlayerSupport;
  artworkSupport?: ClientHelloArtworkSupport;
  visualizerSupport?: ClientHelloVisualizerSupport;
  sourceSupport?: ClientHelloSourceSupport;
  staticDelayMs?: number;
  initialVolume?: number;
  initialMuted?: boolean;
}

export interface ServerInfo {
  serverId: string;
  name: string;
  version: number;
}

const STREAM_CLEAR_ROLES = new Set([Roles.PLAYER, Roles.VISUALIZER]);

export class SendspinClient {
  private readonly clientId: string;
  private readonly clientName: string;
  private readonly roles: Roles[];
  private readonly deviceInfo?: ClientHelloPayload['device_info'];
  private readonly playerSupport?: ClientHelloPlayerSupport;
  private readonly artworkSupport?: ClientHelloArtworkSupport;
  private readonly visualizerSupport?: ClientHelloVisualizerSupport;
  private readonly sourceSupport?: ClientHelloSourceSupport;

  private ws?: WebSocket;
  private connected = false;
  private serverInfo?: ServerInfo;
  private serverHelloResolve?: () => void;
  private timeFilter = new SendspinTimeFilter();
  private streamActive = false;
  private currentAudioFormat?: AudioFormat;
  private currentPlayer?: StreamStartPayload['player'];
  private staticDelayUs = 0;
  private initialVolume: number;
  private initialMuted: boolean;

  private readerClosed = false;
  private timeSyncTimer: NodeJS.Timeout | null = null;

  private metadataCallbacks = new Set<MetadataCallback>();
  private groupCallbacks = new Set<GroupUpdateCallback>();
  private controllerCallbacks = new Set<ControllerStateCallback>();
  private streamStartCallbacks = new Set<StreamStartCallback>();
  private streamEndCallbacks = new Set<StreamEndCallback>();
  private streamClearCallbacks = new Set<StreamClearCallback>();
  private audioChunkCallbacks = new Set<AudioChunkCallback>();
  private disconnectCallbacks = new Set<DisconnectCallback>();
  private serverCommandCallbacks = new Set<ServerCommandCallback>();
  private sourceCommandCallbacks = new Set<SourceCommandCallback>();

  constructor(clientId: string, clientName: string, roles: Roles[], options: SendspinClientOptions = {}) {
    if (roles.includes(Roles.PLAYER) && !options.playerSupport) {
      throw new Error('playerSupport is required when PLAYER role is specified');
    }
    if (roles.includes(Roles.ARTWORK) && !options.artworkSupport) {
      throw new Error('artworkSupport is required when ARTWORK role is specified');
    }
    if (roles.includes(Roles.SOURCE) && !options.sourceSupport) {
      throw new Error('sourceSupport is required when SOURCE role is specified');
    }

    this.clientId = clientId;
    this.clientName = clientName;
    this.roles = [...roles];
    this.deviceInfo = options.deviceInfo;
    this.playerSupport = options.playerSupport;
    this.artworkSupport = options.artworkSupport;
    this.visualizerSupport = options.visualizerSupport;
    this.sourceSupport = options.sourceSupport;
    this.initialVolume = options.initialVolume ?? 100;
    this.initialMuted = options.initialMuted ?? false;
    this.setStaticDelayMs(options.staticDelayMs ?? 0);
  }

  get info(): ServerInfo | undefined {
    return this.serverInfo;
  }

  get isConnected(): boolean {
    return this.connected && !!this.ws && this.ws.readyState === WebSocket.OPEN;
  }

  get staticDelayMs(): number {
    return this.staticDelayUs / 1000;
  }

  setStaticDelayMs(delayMs: number): void {
    const delayUs = Math.round(delayMs * 1000);
    this.staticDelayUs = delayUs;
  }

  async connect(url: string, { timeoutMs = 10000 }: { timeoutMs?: number } = {}): Promise<void> {
    if (this.isConnected) return;

    this.readerClosed = false;
    this.ws = new WebSocket(url);
    this.connected = true;

    const openPromise = new Promise<void>((resolve, reject) => {
      const handleError = (err: Error) => {
        this.ws?.off('open', handleOpen);
        reject(err);
      };
      const handleOpen = () => {
        this.ws?.off('error', handleError);
        resolve();
      };
      this.ws?.once('open', handleOpen);
      this.ws?.once('error', handleError);
    });

    this.ws.on('message', (data) => this.handleWsMessage(data));
    this.ws.on('close', () => this.disconnect());
    this.ws.on('error', () => this.disconnect());

    const timer = setTimeout(() => {
      this.disconnect();
    }, timeoutMs);
    await openPromise.finally(() => clearTimeout(timer));

    await this.sendClientHello();
    await this.waitForServerHello(timeoutMs);

    if (this.roles.includes(Roles.PLAYER)) {
      await this.sendPlayerState({
        state: ClientStateType.SYNCHRONIZED,
        volume: this.initialVolume,
        muted: this.initialMuted,
      });
    }

    await this.sendTimeMessage();
    this.scheduleTimeSync();
  }

  async disconnect(): Promise<void> {
    if (!this.connected && this.ws?.readyState !== WebSocket.OPEN) {
      this.cleanup();
      return;
    }
    this.connected = false;
    this.stopTimeSync();
    if (this.ws && this.ws.readyState === WebSocket.OPEN && !this.readerClosed) {
      this.readerClosed = true;
      await new Promise<void>((resolve) => {
        this.ws?.once('close', resolve);
        this.ws?.close();
        setTimeout(resolve, 100);
      });
    }
    this.cleanup();
  }

  async sendPlayerState(state: PlayerStatePayload): Promise<void> {
    if (!this.isConnected) throw new Error('Client is not connected');
    const { state: clientState, ...playerState } = state as PlayerStatePayload & { state?: ClientStateType };
    const message: ClientStateMessage = {
      type: 'client/state',
      payload: {
        ...(clientState ? { state: clientState } : {}),
        ...(Object.keys(playerState).length ? { player: playerState } : {}),
      },
    };
    await this.sendMessage(message);
  }

  async sendSourceState(state: SourceStatePayload): Promise<void> {
    if (!this.isConnected) throw new Error('Client is not connected');
    const message: ClientStateMessage = {
      type: 'client/state',
      payload: {
        source: state,
      },
    };
    await this.sendMessage(message);
  }

  async sendGroupCommand(command: MediaCommand, opts: { volume?: number; mute?: boolean } = {}): Promise<void> {
    if (!this.isConnected) throw new Error('Client is not connected');
    const controllerPayload: ControllerCommandPayload = { command, volume: opts.volume, mute: opts.mute };
    const payload: ClientCommandPayload = { controller: controllerPayload };
    const message: ClientCommandMessage = { type: 'client/command', payload };
    await this.sendMessage(message);
  }

  async sendSourceCommand(command: SourceClientCommand): Promise<void> {
    if (!this.isConnected) throw new Error('Client is not connected');
    const payload: ClientCommandPayload = { source: { command } };
    const message: ClientCommandMessage = { type: 'client/command', payload };
    await this.sendMessage(message);
  }

  async sendSourceAudioChunk(
    audioData: Buffer,
    opts: { captureTimestampUs?: number; serverTimestampUs?: number } = {},
  ): Promise<void> {
    if (!this.isConnected) throw new Error('Client is not connected');
    const { captureTimestampUs, serverTimestampUs } = opts;
    let timestampUs = serverTimestampUs;
    if (timestampUs == null) {
      if (captureTimestampUs == null) {
        throw new Error('captureTimestampUs or serverTimestampUs must be provided');
      }
      if (!this.timeFilter.isSynchronized) {
        throw new Error('time sync not ready; provide serverTimestampUs explicitly');
      }
      timestampUs = this.computeSourceTimestamp(captureTimestampUs);
    }
    const header = packBinaryHeaderRaw(BinaryMessageType.SOURCE_AUDIO_CHUNK, timestampUs);
    await this.sendBinary(Buffer.concat([header, audioData]));
  }

  addMetadataListener(callback: MetadataCallback): () => void {
    this.metadataCallbacks.add(callback);
    return () => this.metadataCallbacks.delete(callback);
  }

  addGroupUpdateListener(callback: GroupUpdateCallback): () => void {
    this.groupCallbacks.add(callback);
    return () => this.groupCallbacks.delete(callback);
  }

  addControllerStateListener(callback: ControllerStateCallback): () => void {
    this.controllerCallbacks.add(callback);
    return () => this.controllerCallbacks.delete(callback);
  }

  addStreamStartListener(callback: StreamStartCallback): () => void {
    this.streamStartCallbacks.add(callback);
    return () => this.streamStartCallbacks.delete(callback);
  }

  addStreamEndListener(callback: StreamEndCallback): () => void {
    this.streamEndCallbacks.add(callback);
    return () => this.streamEndCallbacks.delete(callback);
  }

  addStreamClearListener(callback: StreamClearCallback): () => void {
    this.streamClearCallbacks.add(callback);
    return () => this.streamClearCallbacks.delete(callback);
  }

  addAudioChunkListener(callback: AudioChunkCallback): () => void {
    this.audioChunkCallbacks.add(callback);
    return () => this.audioChunkCallbacks.delete(callback);
  }

  addDisconnectListener(callback: DisconnectCallback): () => void {
    this.disconnectCallbacks.add(callback);
    return () => this.disconnectCallbacks.delete(callback);
  }

  addServerCommandListener(callback: ServerCommandCallback): () => void {
    this.serverCommandCallbacks.add(callback);
    return () => this.serverCommandCallbacks.delete(callback);
  }

  addSourceCommandListener(callback: SourceCommandCallback): () => void {
    this.sourceCommandCallbacks.add(callback);
    return () => this.sourceCommandCallbacks.delete(callback);
  }

  isTimeSynchronized(): boolean {
    return this.timeFilter.isSynchronized;
  }

  /**
   * When to submit a frame to the audio device, on this client's own clock.
   *
   * `staticDelayUs` is **subtracted**, which is the opposite of what the name suggests and is what
   * the spec requires: "Clients must translate this server timestamp to their local clock using the
   * offset computed from clock synchronization, subtracting their `static_delay_ms` from the
   * timestamp." The value is not an offset that moves playback later — it describes delay that
   * happens *after* the audio port (an amplifier, an active speaker), so playing that much earlier
   * is what makes the sound land on the timestamp.
   *
   * This used to add it, so a positive value played *late* by twice the intended amount relative to
   * a compliant peer: setting 200 ms to bring a lagging room forward pushed it 200 ms further back
   * instead, and two clients built on different libraries moved in opposite directions on the same
   * `set_static_delay`. Verified against the spec (roles/player/v1 timing rules) and against
   * aiosendspin, which subtracts here and adds in `computeServerTime`.
   */
  computePlayTime(serverTimestampUs: number): number {
    if (this.timeFilter.isSynchronized) {
      const clientTime = this.timeFilter.computeClientTime(serverTimestampUs);
      return clientTime - this.staticDelayUs;
    }
    return this.nowUs() + 500_000 - this.staticDelayUs;
  }

  /** The inverse of computePlayTime: add the static delay back before converting to server time. */
  computeServerTime(clientTimestampUs: number): number {
    const adjusted = clientTimestampUs + this.staticDelayUs;
    return this.timeFilter.computeServerTime(adjusted);
  }

  computeSourceTimestamp(captureTimestampUs: number): number {
    return this.timeFilter.computeServerTime(captureTimestampUs);
  }

  private async waitForServerHello(timeoutMs: number): Promise<void> {
    if (this.serverInfo) return;
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Timed out waiting for server/hello'));
      }, timeoutMs);
      this.serverHelloResolve = () => {
        clearTimeout(timeout);
        resolve();
      };
    });
  }

  private async sendClientHello(): Promise<void> {
    const payload: ClientHelloPayload = {
      client_id: this.clientId,
      name: this.clientName,
      version: 1,
      supported_roles: this.roles,
      device_info: this.deviceInfo,
      ['player@v1_support']: this.roles.includes(Roles.PLAYER) ? this.playerSupport : undefined,
      ['artwork@v1_support']: this.roles.includes(Roles.ARTWORK) ? this.artworkSupport : undefined,
      ['visualizer@_draft_r1_support']: this.roles.includes(Roles.VISUALIZER)
        ? this.visualizerSupport
        : undefined,
      ['source@v1_support']: this.roles.includes(Roles.SOURCE) ? this.sourceSupport : undefined,
    };
    const message: ClientHelloMessage = { type: 'client/hello', payload };
    await this.sendMessage(message);
  }

  private async sendTimeMessage(): Promise<void> {
    if (!this.isConnected) return;
    const nowUs = this.nowUs();
    const payload: ClientTimePayload = { client_transmitted: nowUs };
    const message: ClientTimeMessage = { type: 'client/time', payload };
    await this.sendMessage(message);
  }

  private async sendMessage(message: ClientOutboundMessage): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not connected');
    }
    const data = JSON.stringify(message);
    this.ws.send(data);
  }

  private async sendBinary(payload: Buffer): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('WebSocket is not connected');
    }
    this.ws.send(payload);
  }

  private handleWsMessage(data: WebSocket.RawData): void {
    if (typeof data === 'string') {
      void this.handleJsonMessage(data);
      return;
    }
    if (data instanceof Buffer || data instanceof Uint8Array) {
      this.handleBinaryMessage(Buffer.from(data));
      return;
    }
  }

  private async handleJsonMessage(raw: string): Promise<void> {
    let message: ServerInboundMessage & { type: string };
    try {
      message = JSON.parse(raw);
    } catch (err) {
      return;
    }

    switch (message.type) {
      case 'server/hello':
        this.handleServerHello((message as ServerHelloMessage).payload);
        break;
      case 'server/time':
        this.handleServerTime((message as ServerTimeMessage).payload);
        break;
      case 'stream/start':
        await this.handleStreamStart(message as StreamStartMessage);
        break;
      case 'stream/clear':
        this.handleStreamClear(message as StreamClearMessage);
        break;
      case 'stream/end':
        this.handleStreamEnd(message as StreamEndMessage);
        break;
      case 'group/update':
        this.handleGroupUpdate((message as GroupUpdateServerMessage).payload);
        break;
      case 'server/state':
        this.handleServerState((message as ServerStateMessage).payload);
        break;
      case 'server/command':
        this.handleServerCommand((message as ServerCommandMessage).payload);
        break;
      default:
        break;
    }
  }

  private handleBinaryMessage(payload: Buffer): void {
    let header;
    try {
      header = unpackBinaryHeader(payload);
    } catch (err) {
      return;
    }

    const messageType = header.messageType;
    if (!this.streamActive) return;

    if (messageType === BinaryMessageType.AUDIO_CHUNK) {
      const chunk = payload.subarray(BINARY_HEADER_SIZE);
      this.handleAudioChunk(header.timestampUs, chunk);
    }
  }

  private handleServerHello(payload: ServerHelloPayload): void {
    this.serverInfo = {
      serverId: payload.server_id,
      name: payload.name,
      version: payload.version,
    };
    this.serverHelloResolve?.();
    this.serverHelloResolve = undefined;
  }

  private handleServerTime(payload: ServerTimePayload): void {
    const nowUs = this.nowUs();
    const offset = ((payload.server_received - payload.client_transmitted) + (payload.server_transmitted - nowUs)) / 2;
    const delay = ((nowUs - payload.client_transmitted) - (payload.server_transmitted - payload.server_received)) / 2;
    this.timeFilter.update(Math.round(offset), Math.round(delay), nowUs);
  }

  private async handleStreamStart(message: StreamStartMessage): Promise<void> {
    const player = message.payload.player;
    if (!player) return;

    const isFormatUpdate = this.streamActive && this.currentPlayer;
    this.streamActive = true;

    const pcmFormat = new PCMFormat(player.sample_rate, player.channels, player.bit_depth);
    const codecHeader = player.codec_header ? Buffer.from(player.codec_header, 'base64') : undefined;
    this.configureAudioOutput(new AudioFormat(player.codec, pcmFormat, codecHeader));
    this.currentPlayer = player;

    if (!isFormatUpdate) {
      this.notifyStreamStart(message);
      await this.sendTimeMessage();
    }
  }

  private handleStreamClear(message: StreamClearMessage): void {
    const roles = message.payload.roles as Roles[] | undefined;
    if (roles) {
      const invalid = roles.filter((role) => !STREAM_CLEAR_ROLES.has(role));
      if (invalid.length) return;
    }
    this.notifyStreamClear(roles);
  }

  private handleStreamEnd(message: StreamEndMessage): void {
    const roles = message.payload.roles;
    if (!roles || roles.includes(Roles.PLAYER)) {
      this.streamActive = false;
      this.currentPlayer = undefined;
      this.currentAudioFormat = undefined;
    }
    this.notifyStreamEnd(roles);
  }

  private handleGroupUpdate(payload: GroupUpdateServerPayload): void {
    for (const cb of [...this.groupCallbacks]) {
      try {
        cb(payload);
      } catch (err) {
        // ignore individual callback failures
      }
    }
  }

  private handleServerState(payload: ServerStatePayload): void {
    for (const cb of [...this.controllerCallbacks]) {
      try {
        cb(payload);
      } catch (err) {
        // ignore
      }
    }
    if (payload.metadata) {
      for (const cb of [...this.metadataCallbacks]) {
        try {
          cb(payload);
        } catch (err) {
          // ignore
        }
      }
    }
  }

  private handleServerCommand(payload: ServerCommandPayload): void {
    for (const cb of [...this.serverCommandCallbacks]) {
      try {
        cb(payload);
      } catch {
        // ignore
      }
    }
    if (payload.source) {
      for (const cb of [...this.sourceCommandCallbacks]) {
        try {
          cb(payload.source);
        } catch {
          // ignore
        }
      }
    }
  }

  private configureAudioOutput(format: AudioFormat): void {
    this.currentAudioFormat = format;
  }

  private handleAudioChunk(timestampUs: number, payload: Buffer): void {
    if (!this.currentAudioFormat) return;
    for (const cb of [...this.audioChunkCallbacks]) {
      try {
        cb(timestampUs, payload, this.currentAudioFormat);
      } catch {
        // ignore individual callback failures
      }
    }
  }

  private notifyStreamStart(message: StreamStartMessage): void {
    for (const cb of [...this.streamStartCallbacks]) {
      try {
        cb(message);
      } catch {
        // ignore
      }
    }
  }

  private notifyStreamEnd(roles: RoleName[] | undefined): void {
    for (const cb of [...this.streamEndCallbacks]) {
      try {
        cb(roles);
      } catch {
        // ignore
      }
    }
  }

  private notifyStreamClear(roles: RoleName[] | undefined): void {
    for (const cb of [...this.streamClearCallbacks]) {
      try {
        cb(roles);
      } catch {
        // ignore
      }
    }
  }

  private notifyDisconnect(): void {
    for (const cb of [...this.disconnectCallbacks]) {
      try {
        cb();
      } catch {
        // ignore
      }
    }
  }

  private scheduleTimeSync(): void {
    this.stopTimeSync();
    const interval = this.computeTimeSyncInterval();
    this.timeSyncTimer = setTimeout(async () => {
      try {
        await this.sendTimeMessage();
      } catch {
        // ignore send failures to keep loop alive
      }
      this.scheduleTimeSync();
    }, interval);
  }

  private stopTimeSync(): void {
    if (this.timeSyncTimer) {
      clearTimeout(this.timeSyncTimer);
      this.timeSyncTimer = null;
    }
  }

  private computeTimeSyncInterval(): number {
    if (!this.timeFilter.isSynchronized) return 200;
    const error = this.timeFilter.error;
    if (error < 1_000) return 3000;
    if (error < 2_000) return 1000;
    if (error < 5_000) return 500;
    return 200;
  }

  private nowUs(): number {
    return Math.floor(performance.now() * 1000);
  }

  private cleanup(): void {
    this.stopTimeSync();
    this.ws?.removeAllListeners();
    this.ws = undefined;
    this.timeFilter.reset();
    this.serverInfo = undefined;
    this.streamActive = false;
    this.currentAudioFormat = undefined;
    this.currentPlayer = undefined;
    this.notifyDisconnect();
  }
}
