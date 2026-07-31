/**
 * Core Sendspin protocol enums and message payload types.
 * These mirror the reference `aiosendspin` Python library but are expressed
 * as TypeScript interfaces so they can be used directly with JSON.
 */

export enum Roles {
  PLAYER = 'player@v1',
  CONTROLLER = 'controller@v1',
  METADATA = 'metadata@v1',
  ARTWORK = 'artwork@v1',
  /**
   * Visualizer role tracks the upstream Sendspin spec, which still ships the
   * visualizer role as a draft revision (`visualizer@_draft_r1`). Updating
   * here keeps role negotiation working with spec-compliant clients
   * (esphome, sendspin-cli, aiosendspin).
   */
  VISUALIZER = 'visualizer@_draft_r1',
  /**
   * Current spec revision of the visualizer role. Uses per-type binary frames
   * (loudness/f_peak/spectrum/beat/peak/pitch, message types 16-21) rather
   * than the draft's batched DATA blob. The latest sendspin-cli advertises
   * only this version and hides its panel unless the server activates it.
   */
  VISUALIZER_V1 = 'visualizer@v1',
  /**
   * Outbound-only role: the server pushes a color palette derived from the
   * current artwork via `server/state`. No client/hello support object is
   * required — clients simply list `color@v1` in `supported_roles`.
   */
  COLOR = 'color@v1',
  /**
   * Vendor extension (Lox-Audioserver only): used to receive line-in audio
   * from ESPHome devices via SOURCE_AUDIO_CHUNK. Not part of the upstream
   * Sendspin spec — keep aware of this when bumping protocol versions.
   */
  SOURCE = 'source@v1',
}

export type RoleName = Roles | string;

export enum BinaryMessageType {
  AUDIO_CHUNK = 4,
  ARTWORK_CHANNEL_0 = 8,
  ARTWORK_CHANNEL_1 = 9,
  ARTWORK_CHANNEL_2 = 10,
  ARTWORK_CHANNEL_3 = 11,
  /** Vendor extension: not in upstream spec. Used for line-in ingest. */
  SOURCE_AUDIO_CHUNK = 12,
  /**
   * Slot 16 is shared: the legacy `visualizer@_draft_r1` wire sends a batched
   * DATA blob here, while `visualizer@v1` sends a single loudness frame. The
   * negotiated role selects the framing, so both names map to byte 16.
   */
  VISUALIZATION_DATA = 16,
  VISUALIZATION_LOUDNESS = 16,
  VISUALIZATION_BEAT = 17,
  VISUALIZATION_F_PEAK = 18,
  VISUALIZATION_SPECTRUM = 19,
  VISUALIZATION_PEAK = 20,
  VISUALIZATION_PITCH = 21,
}

export enum RepeatMode {
  OFF = 'off',
  ONE = 'one',
  ALL = 'all',
}

export enum ClientStateType {
  SYNCHRONIZED = 'synchronized',
  ERROR = 'error',
  EXTERNAL_SOURCE = 'external_source',
}

export enum SourceStateType {
  IDLE = 'idle',
  STREAMING = 'streaming',
  ERROR = 'error',
}

export enum SourceSignalType {
  UNKNOWN = 'unknown',
  PRESENT = 'present',
  ABSENT = 'absent',
}

export enum PlaybackStateType {
  PLAYING = 'playing',
  PAUSED = 'paused',
  STOPPED = 'stopped',
}

export enum AudioCodec {
  OPUS = 'opus',
  FLAC = 'flac',
  PCM = 'pcm',
}

export enum PlayerCommand {
  VOLUME = 'volume',
  MUTE = 'mute',
  SET_STATIC_DELAY = 'set_static_delay',
}

export enum MediaCommand {
  PLAY = 'play',
  PAUSE = 'pause',
  STOP = 'stop',
  NEXT = 'next',
  PREVIOUS = 'previous',
  VOLUME = 'volume',
  MUTE = 'mute',
  REPEAT_OFF = 'repeat_off',
  REPEAT_ONE = 'repeat_one',
  REPEAT_ALL = 'repeat_all',
  SHUFFLE = 'shuffle',
  UNSHUFFLE = 'unshuffle',
  SWITCH = 'switch',
  SELECT_SOURCE = 'select_source',
}

export enum SourceCommand {
  START = 'start',
  STOP = 'stop',
}

export enum SourceControl {
  PLAY = 'play',
  PAUSE = 'pause',
  NEXT = 'next',
  PREVIOUS = 'previous',
  ACTIVATE = 'activate',
  DEACTIVATE = 'deactivate',
}

export enum SourceClientCommand {
  STARTED = 'started',
  STOPPED = 'stopped',
}

export enum PictureFormat {
  BMP = 'bmp',
  JPEG = 'jpeg',
  PNG = 'png',
}

export enum ArtworkSource {
  ALBUM = 'album',
  ARTIST = 'artist',
  NONE = 'none',
}

export enum ConnectionReason {
  DISCOVERY = 'discovery',
  PLAYBACK = 'playback',
}

export enum GoodbyeReason {
  ANOTHER_SERVER = 'another_server',
  SHUTDOWN = 'shutdown',
  RESTART = 'restart',
  USER_REQUEST = 'user_request',
}

export type UndefinedField = typeof UNDEFINED_FIELD;
export const UNDEFINED_FIELD = Symbol('sendspin/undefined');
export const undefinedField = (): UndefinedField => UNDEFINED_FIELD;
export const isUndefinedField = (
  value: unknown,
): value is UndefinedField => value === UNDEFINED_FIELD;

export interface DeviceInfo {
  product_name?: string | null;
  manufacturer?: string | null;
  software_version?: string | null;
}

export interface SupportedAudioFormat {
  codec: AudioCodec;
  channels: number;
  sample_rate: number;
  bit_depth: number;
}

export interface ClientHelloPlayerSupport {
  supported_formats: SupportedAudioFormat[];
  buffer_capacity: number;
  supported_commands: PlayerCommand[];
}

export interface SourceFormat {
  codec: AudioCodec;
  channels: number;
  sample_rate: number;
  bit_depth: number;
}

export interface SourceFeatures {
  level?: boolean;
  line_sense?: boolean;
}

export interface ClientHelloSourceSupport {
  supported_formats: SourceFormat[];
  controls?: SourceControl[];
  features?: SourceFeatures;
}

export interface SourceVadSettings {
  threshold_db?: number;
  hold_ms?: number;
}

export interface SourceCommandPayload {
  command?: SourceCommand;
  control?: SourceControl;
  vad?: SourceVadSettings;
}

export interface SourceClientCommandPayload {
  command: SourceClientCommand;
}

export interface ArtworkChannel {
  source: ArtworkSource;
  format: PictureFormat;
  media_width: number;
  media_height: number;
}

export interface ClientHelloArtworkSupport {
  channels: ArtworkChannel[];
}

export interface ClientHelloVisualizerSupport {
  buffer_capacity: number;
}

export interface StreamArtworkChannelConfig {
  source: ArtworkSource;
  format: PictureFormat;
  width: number;
  height: number;
}

export interface StreamStartArtwork {
  channels: StreamArtworkChannelConfig[];
}

export interface StreamRequestFormatArtwork {
  channel: number;
  source?: ArtworkSource;
  format?: PictureFormat;
  media_width?: number;
  media_height?: number;
}

export interface StreamStartVisualizer {
  // Placeholder for spec parity
}

export interface Progress {
  track_progress: number;
  track_duration: number;
  playback_speed: number;
}

export interface SessionUpdateMetadata {
  timestamp: number;
  title?: string | null | UndefinedField;
  artist?: string | null | UndefinedField;
  album_artist?: string | null | UndefinedField;
  album?: string | null | UndefinedField;
  artwork_url?: string | null | UndefinedField;
  year?: number | null | UndefinedField;
  track?: number | null | UndefinedField;
  progress?: Progress | null | UndefinedField;
  repeat?: RepeatMode | null | UndefinedField;
  shuffle?: boolean | null | UndefinedField;
}

export interface ControllerCommandPayload {
  command: MediaCommand;
  volume?: number;
  mute?: boolean;
  source_id?: string | null;
}

export interface ControllerStatePayload {
  supported_commands: MediaCommand[];
  volume: number;
  muted: boolean;
  sources?: Array<{
    id: string;
    name: string;
    state: SourceStateType;
    signal?: SourceSignalType | null;
    selected?: boolean | null;
    last_event?: SourceClientCommand | null;
    last_event_ts_us?: number | null;
  }>;
}

export interface ClientHelloPayload {
  client_id: string;
  name: string;
  version: number;
  supported_roles: RoleName[];
  device_info?: DeviceInfo;
  ['player@v1_support']?: ClientHelloPlayerSupport;
  ['artwork@v1_support']?: ClientHelloArtworkSupport;
  /** Current spec key: `visualizer@_draft_r1_support`. */
  ['visualizer@_draft_r1_support']?: ClientHelloVisualizerSupport;
  /** Legacy compat key for clients still emitting the old `visualizer@v1` role. */
  ['visualizer@v1_support']?: ClientHelloVisualizerSupport;
  ['source@v1_support']?: ClientHelloSourceSupport;
}

export interface ClientHelloMessage {
  type: 'client/hello';
  payload: ClientHelloPayload;
}

export interface ClientTimePayload {
  client_transmitted: number;
}

export interface ClientTimeMessage {
  type: 'client/time';
  payload: ClientTimePayload;
}

export interface PlayerStatePayload {
  state?: ClientStateType;
  volume?: number;
  muted?: boolean;
  /**
   * Static delay in milliseconds (0-5000). REQUIRED for players in the initial state message.
   *
   * The delay the client's own chain adds *after* its audio port — an amplifier, an active speaker.
   * The client subtracts it from every timestamp, so a server must add it to how far ahead it sends
   * or the setting is paid for out of the buffer instead (spec: "Servers factor in each client's
   * static_delay_ms when calculating how far ahead to send audio, keeping effective buffer headroom
   * constant"). The client owns and persists this value; `set_static_delay` only asks.
   */
  static_delay_ms?: number;
  /**
   * Minimum startup lead time in milliseconds (0-30000). REQUIRED for players initially.
   *
   * Codec init, decode warmup, backend buffering, DAC latency — measured from the server transmit
   * time of the start trigger to the playback timestamp of the first chunk that can play in full.
   * A hint: the server MAY give less. Excludes `static_delay_ms`.
   */
  required_lead_time_ms?: number;
  /**
   * Requested minimum ongoing buffer during playback, in milliseconds (0-30000). REQUIRED initially.
   *
   * Absorbs network jitter and decode variance, mainly for live streams. Excludes
   * `static_delay_ms`.
   */
  min_buffer_ms?: number;
  /** Subset of 'set_static_delay': which of these the client will accept from the server. */
  supported_commands?: PlayerCommand[];
}

export interface SourceStatePayload {
  state: SourceStateType;
  level?: number;
  signal?: SourceSignalType;
}

export interface ClientStatePayload {
  state?: ClientStateType;
  player?: PlayerStatePayload;
  source?: SourceStatePayload;
}

export interface ClientStateMessage {
  type: 'client/state';
  payload: ClientStatePayload;
}

export interface ClientCommandPayload {
  controller?: ControllerCommandPayload;
  source?: SourceClientCommandPayload;
}

export interface ClientCommandMessage {
  type: 'client/command';
  payload: ClientCommandPayload;
}

export interface ClientGoodbyePayload {
  reason: GoodbyeReason;
}

export interface ClientGoodbyeMessage {
  type: 'client/goodbye';
  payload: ClientGoodbyePayload;
}

export interface StreamRequestFormatPayload {
  player?: StreamRequestFormatPlayer;
  artwork?: StreamRequestFormatArtwork;
}

export interface StreamRequestFormatMessage {
  type: 'stream/request-format';
  payload: StreamRequestFormatPayload;
}

export interface StreamRequestFormatPlayer {
  codec?: AudioCodec;
  sample_rate?: number;
  channels?: number;
  bit_depth?: number;
}

export interface ServerHelloPayload {
  server_id: string;
  name: string;
  version: number;
  active_roles: RoleName[];
  connection_reason: ConnectionReason;
}

export interface ServerHelloMessage {
  type: 'server/hello';
  payload: ServerHelloPayload;
}

export interface ServerTimePayload {
  client_transmitted: number;
  server_received: number;
  server_transmitted: number;
}

export interface ServerTimeMessage {
  type: 'server/time';
  payload: ServerTimePayload;
}

export type VisualizerType = 'loudness' | 'f_peak' | 'spectrum' | 'beat' | 'peak' | 'pitch';
export type SpectrumScale = 'lin' | 'log' | 'mel';

/** Spectrum configuration shared by client/hello support and stream/start. */
export interface VisualizerSpectrumConfig {
  n_disp_bins: number;
  scale: SpectrumScale;
  f_min: number;
  f_max: number;
}

/** Parsed visualizer@v1 support object from client/hello. */
export interface VisualizerSupport {
  buffer_capacity: number;
  rate_max: number;
  types: VisualizerType[];
  spectrum?: VisualizerSpectrumConfig;
}

/** Negotiated visualizer config echoed back in stream/start. */
export interface VisualizerStreamConfig {
  types: VisualizerType[];
  rate_max: number;
  spectrum?: VisualizerSpectrumConfig;
  tracks_downbeats?: boolean;
}

/** An sRGB color as `[R, G, B]`, each component 0-255. */
export type Rgb = [number, number, number];

/**
 * Color object in a `server/state` message (color@v1). Each field is an
 * `[R, G, B]` tuple, `null` to explicitly clear it, or omitted to leave it
 * unchanged. The spec mandates WCAG >=4.5:1 contrast for the background/on
 * pairs; the caller is responsible for honoring that when building the value.
 */
export interface SessionUpdateColor {
  timestamp: number;
  background_dark?: Rgb | null | UndefinedField;
  background_light?: Rgb | null | UndefinedField;
  primary?: Rgb | null | UndefinedField;
  accent?: Rgb | null | UndefinedField;
  on_dark?: Rgb | null | UndefinedField;
  on_light?: Rgb | null | UndefinedField;
}

export interface ServerStatePayload {
  metadata?: SessionUpdateMetadata;
  controller?: ControllerStatePayload;
  color?: SessionUpdateColor;
}

export interface ServerStateMessage {
  type: 'server/state';
  payload: ServerStatePayload;
}

export interface GroupUpdateServerPayload {
  playback_state?: PlaybackStateType;
  group_id?: string;
  group_name?: string;
}

export interface GroupUpdateServerMessage {
  type: 'group/update';
  payload: GroupUpdateServerPayload;
}

export interface StreamStartPlayer {
  codec: AudioCodec;
  sample_rate: number;
  channels: number;
  bit_depth: number;
  codec_header?: string | null;
}

export interface StreamStartPayload {
  player?: StreamStartPlayer;
  artwork?: StreamStartArtwork;
  visualizer?: StreamStartVisualizer;
}

export interface StreamStartMessage {
  type: 'stream/start';
  payload: StreamStartPayload;
}

export interface StreamClearPayload {
  roles?: RoleName[];
}

export interface StreamClearMessage {
  type: 'stream/clear';
  payload: StreamClearPayload;
}

export interface StreamEndPayload {
  roles?: RoleName[];
}

export interface StreamEndMessage {
  type: 'stream/end';
  payload: StreamEndPayload;
}

export interface PlayerCommandPayload {
  command: PlayerCommand;
  volume?: number;
  mute?: boolean;
  /** Static playback delay in milliseconds (0-5000), only valid when command is `set_static_delay`. */
  static_delay_ms?: number;
}

export interface ServerCommandPayload {
  player?: PlayerCommandPayload;
  source?: SourceCommandPayload;
}

export interface ServerCommandMessage {
  type: 'server/command';
  payload: ServerCommandPayload;
}

export type ClientOutboundMessage =
  | ClientHelloMessage
  | ClientTimeMessage
  | ClientStateMessage
  | ClientCommandMessage
  | ClientGoodbyeMessage
  | StreamRequestFormatMessage;

export type ServerInboundMessage =
  | ServerHelloMessage
  | ServerTimeMessage
  | ServerStateMessage
  | GroupUpdateServerMessage
  | StreamStartMessage
  | StreamClearMessage
  | StreamEndMessage
  | ServerCommandMessage;

export type ServerOutboundMessage =
  | ServerHelloMessage
  | ServerTimeMessage
  | ServerStateMessage
  | GroupUpdateServerMessage
  | StreamStartMessage
  | StreamClearMessage
  | StreamEndMessage
  | ServerCommandMessage;

export type ClientInboundMessage =
  | ClientHelloMessage
  | ClientTimeMessage
  | ClientStateMessage
  | ClientCommandMessage
  | ClientGoodbyeMessage
  | StreamRequestFormatMessage;
