# node-sendspin

TypeScript/Node.js implementation of the Sendspin protocol. It exposes both client and server building blocks and is primarily used by Sonn core, but is generic enough for other Sendspin deployments.

## Install

```sh
npm install @sonn-audio/node-sendspin
```

## Quickstart

```ts
import {
  SendspinClient,
  Roles,
  AudioCodec,
  MediaCommand,
} from '@sonn-audio/node-sendspin';

const client = new SendspinClient('my-client-id', 'My Player', [Roles.PLAYER], {
  playerSupport: {
    supported_formats: [
      { codec: AudioCodec.PCM, channels: 2, sample_rate: 48000, bit_depth: 16 },
    ],
    buffer_capacity: 512 * 1024,
    supported_commands: [],
  },
  staticDelayMs: 75,
});

client.addStreamStartListener(() => console.log('Stream started'));
client.addAudioChunkListener((timestampUs, data, format) => {
  const playAt = client.computePlayTime(timestampUs);
  // schedule playback of `data` (PCM) at `playAt` microseconds on your clock
});

await client.connect('ws://localhost:8927/sendspin');

// Send playback commands to the group
await client.sendGroupCommand(MediaCommand.PLAY);
```

### Simple server

```ts
import { SendspinServer } from '@sonn-audio/node-sendspin';

const server = new SendspinServer('server-id', 'My Sendspin Server');
server.on('client-added', (evt) => console.log('client connected', evt.clientId));
await server.start({ port: 8927, path: '/sendspin' });
```

## API highlights

- Protocol enums and payload types mirror the Sendspin spec.
- `SendspinTimeFilter` provides Kalman-filtered clock sync (microsecond precision) and is reused by the client.
- `SendspinClient` handles WebSocket handshake (`client/hello`, `server/hello`), periodic `client/time` sync, stream lifecycle messages, and audio chunks (binary `AUDIO_CHUNK`) with codec/format info.
- Helper utilities for packing/unpacking binary headers.
- Server-side building blocks:
  - `SendspinServer`/`ServerClient` to host a WebSocket endpoint or connect to clients.
  - `SendspinCore`/`SendspinSession` to manage sessions, push stream/state/metadata/commands, and send PCM/artwork/visualizer frames with backpressure guards.
  - Player role requires an initial `client/state` update before the session is fully identified.

## Building

```sh
npm install
npm run build
```

Compiled artifacts land in `dist/` with type declarations for publishing to npm.
