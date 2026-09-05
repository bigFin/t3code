# Remote architecture

Each connection joins a client to one environment over HTTP and WebSocket. The
environment owns providers, execution, files, and durable state. Direct access,
Tailscale, SSH, and T3 Connect change how the client reaches that server; they do
not introduce another execution model. See
[remote access](../user/remote-access.md) for setup.

## Identity is independent of the route

An environment keeps its ID across server restarts and endpoint changes. Saved
connections are local to a client profile; the server's identity and state are
not. A repository identity can correlate clones across environments, but never
routes work between them. A project and its threads belong to one environment.

[Environment ID initialization](../../apps/server/src/environment/ServerEnvironment.ts)
must publish a complete ID atomically. Repair of an empty ID file retains a
recovery file so concurrent or delayed initializers choose the same winner.
Removing that recovery state as ordinary temporary-file cleanup can change the
identity underneath an already-running server.

Advertised endpoints are reachability hints. Only the connecting device can
prove that a route works. In particular, a host's loopback address refers to a
different machine when another device opens it. Endpoint selection must not
silently fall back to loopback when a shareable endpoint is unavailable.

## Hosted web is a client

The hosted web app stores its connection catalog in the browser and connects
directly to each environment. It does not proxy traffic or hold server-side
pairing state. Hosting the UI over HTTPS therefore cannot make a plain HTTP LAN
backend accessible from that browser context.

A [hosted pairing URL](../../apps/web/src/hostedPairing.ts) identifies the backend
in its query and carries the pairing secret in its fragment. Fragments stay out
of requests to the hosted origin. The browser exchanges the secret with the
environment and strips it from its history. Moving the token into a query
parameter would disclose it to the wrong origin.

## Access and process ownership are different

Tailscale supplies an endpoint for ordinary pairing, so it needs no separate
environment type. Authentication remains the environment's responsibility for
every route. See [environment authentication](./environment-auth.md) and the
[T3 Connect trust boundary](./t3-connect.md).

SSH can launch a server as well as forward a port. Desktop main owns that
lifecycle because it can spawn SSH and handle authentication prompts. The
renderer uses the forwarded endpoint through the shared connection runtime.
[SSH cleanup](../../packages/ssh/src/tunnel.ts) stops a remote server only if the
launcher owns it; a server it discovered already running must survive a client
disconnect. Reconnection restores the forward before opening the application
transport.

[`connection/model.ts`][model] defines four target tags, which are the real access taxonomy:

| Target                    | Used for                                                                 |
| ------------------------- | ------------------------------------------------------------------------ |
| `PrimaryConnectionTarget` | The platform-managed local server (desktop backend, CLI-served web app). |
| `BearerConnectionTarget`  | Any manually paired endpoint reached over direct HTTP/WebSocket.         |
| `RelayConnectionTarget`   | Managed T3 Connect relay tunnels.                                        |
| `SshConnectionTarget`     | Desktop-managed SSH environments.                                        |

Bearer, relay, and SSH are persisted; primary is platform-managed. Note that Tailscale is not a
separate target kind. A Tailscale URL is paired through the ordinary bearer path in
[`onboarding.ts`][onboarding] (`preparePairingRegistration`), which accepts either a pairing URL or a
host plus pairing code. Tailscale is an endpoint provider and transport, not a distinct runtime
concept.

### AdvertisedEndpoint

A server- or desktop-authored candidate endpoint for an environment: a concrete HTTP and WebSocket
base URL pair, a default/available/unavailable marker, reachability hints (loopback, LAN, private,
public, tunnel), and compatibility hints such as whether the hosted HTTPS app can use it.

Clients treat advertised endpoints as hints, not proof that a route works from the current device.
The connection attempt decides.

The UI shows one default endpoint in the network-access summary and keeps the rest behind an advanced
list. `selectPairingEndpoint` in
[`ConnectionsSettings.tsx`](../../apps/web/src/components/settings/ConnectionsSettings.tsx) excludes
unavailable endpoints and then picks, in order:

1. the saved `defaultEndpointKey` override;
2. the first endpoint marked `isDefault`;
3. the first endpoint whose reachability is not `loopback`;
4. the first endpoint compatible with the hosted HTTPS app;
5. otherwise nothing.

There is no unconditional loopback fallback. A loopback endpoint only wins through an explicit saved
override or `isDefault`. Persist the override by stable endpoint kind rather than raw URL where
possible, since LAN addresses change with networks; Tailscale endpoints use provider-specific stable
keys (`tailscale-ip:`, `tailscale-magicdns:`).

### Endpoint providers

Endpoint providers contribute advertised endpoints without becoming part of the core environment
model: core owns environments, pairing, and connection lifecycle, and providers return normalized
`AdvertisedEndpoint` records.

Tailscale is the first provider, and T3 manages more than discovery. When `tailscaleServeEnabled` is
set, the server acquires a Tailscale serve mapping for its actual listening port at startup with
`ensureTailscaleServe` and releases it with `disableTailscaleServe` on scope close
(`apps/server/src/server.ts`, using [`@t3tools/tailscale`](../../packages/tailscale/src/tailscale.ts)).
Endpoint identifiers are synthesized in `apps/desktop/src/backend/tailscaleEndpointProvider.ts` with
`private-network` reachability.

### Hosted pairing request

A hosted pairing request is a bootstrap URL for the static web app, not a transport:

```text
https://app.t3.codes/pair?host=https://backend.example.com:3773#token=PAIRCODE
```

The hosted app reads `host`, takes the token from the URL hash, exchanges it directly with that
backend, strips the token from browser history, and saves the environment record locally. Helpers
live in [`shared/remote.ts`](../../packages/shared/src/remote.ts) (`setPairingTokenOnUrl`,
`getPairingTokenFromUrl`, `stripPairingTokenFromUrl`) and `apps/web/src/hostedPairing.ts`.

Constraints:

- the hosted app does not proxy HTTP or WebSocket traffic;
- the backend must be directly reachable from the browser;
- HTTPS pages can only reach HTTPS/WSS backends;
- HTTP LAN endpoints keep using direct desktop or CLI pairing URLs;
- the token belongs in the hash so it is never sent to the hosted app origin.

### RepositoryIdentity and Project

`RepositoryIdentity` is a best-effort logical repo grouping across environments, used for UI grouping
and correlation only, never for routing. `Project` remains environment-local: a local clone and a
remote clone are different projects that may share a `RepositoryIdentity`, and threads bind to one
project in one environment.

## Access methods

Access answers one question: how does the client speak WebSocket to a T3 server? It does not answer
how the server got started or who manages the process.

### Direct WebSocket access

`wss://t3.example.com` or `ws://10.0.0.15:3773`, paired as a bearer target. This is the base model.
It works for desktop, mobile, and web with no client-side process management. Browser security rules
are part of it: a hosted HTTPS client cannot connect to plain `ws://` or `http://` LAN backends.

### Relay-tunneled access

Managed T3 Connect relay tunnels use `RelayConnectionTarget` and are the answer when the host is
behind NAT, inbound ports are unavailable, or mobile must reach a desktop-hosted environment. From
the client's perspective this is still an ordinary WebSocket connection; the route is mediated. The
relay Worker only brokers credentials and a managed endpoint; application traffic then flows over
the provisioned Cloudflare tunnel hostname for the life of the connection, not through the relay
Worker itself. See [t3-connect.md](./t3-connect.md).

### Tailscale access

A T3-managed `tailscale serve` mapping exposes the server on the tailnet over HTTPS, and the
resulting private-network endpoints are advertised for pairing. Connection then follows the ordinary
bearer path.

### Desktop-managed SSH access

SSH is an access and launch helper, not a separate environment type. `DesktopSshEnvironment`
([apps/desktop/src/ssh/DesktopSshEnvironment.ts][sshenv]) exposes `discoverHosts`,
`ensureEnvironment`, and `disconnectEnvironment`. It discovers targets from SSH config and known
hosts, owns password/askpass prompts, and delegates lifecycle to `SshEnvironmentManager` in
[packages/ssh/src/tunnel.ts][sshtunnel], which resolves the target, launches or reuses the remote T3
server, opens a local tunnel, checks HTTP readiness, optionally issues a remote pairing token, and
returns local HTTP/WS endpoints. Disconnect closes the tunnel and stops the remote server if the
launcher started it; a server that was already running (marked `external`) is left running.

The desktop main process owns this because it can spawn SSH, manage prompts, write launch scripts,
and clean up forwards. The renderer connects through the forwarded URL like any other environment and
needs no SSH-specific RPC path.

Application features stay transport-neutral: they target the ordinary environment HTTP/WebSocket
contract and must not add an SSH-specific implementation. If a feature fails only through SSH,
treat that as a remote server version, capability, or connection-lifecycle problem rather than
adding a parallel RPC path.

Failure handling is explicit: SSH auth failure surfaces before an environment is saved, remote launch
failure includes launcher output where available, forwarded-port failure leaves the environment
disconnected rather than falling back to an unrelated endpoint, and reconnect restores the SSH bridge
before reconnecting the WebSocket client.

## Launch methods

Launch answers a different question: how does a T3 server come to exist on the target machine? Keep
it separate from access.

- **Pre-existing server.** The operator already runs T3 and the client connects directly or through a
  tunnel.
- **Desktop-managed remote launch over SSH.** Desktop probes the machine, launches or reuses a remote
  server, forwards a port, and the renderer connects normally. The saved environment records that it
  came from SSH launch for reconnect and lifecycle UX only; that metadata never changes the protocol
  or the identity model.
- **Client-managed local publish.** A local server is published through the relay with
  `t3 connect link`, exposing a desktop-hosted environment to mobile without router or firewall
  changes.

The same `ExecutionEnvironment` can be reached several of these ways. Only the launch and access
paths differ.

## Security model

Some environments are reachable over untrusted networks, so remote-capable environments require
explicit authentication, tunnel exposure never relies on obscurity, and saved endpoints carry enough
auth metadata to reconnect safely.

WebSocket authentication is a dedicated short-lived ticket, not a token in a query string. The client
presents its long-lived bearer or DPoP credential in HTTP headers to
`POST /api/auth/websocket-ticket` ([authorization/remote.ts][authremote]), and appends only the
returned ticket as `wsTicket` on the socket URL. The server issues it through
`EnvironmentAuth.issueWebSocketTicket`; tickets are tagged `kind: "websocket"` and default to a
five-minute TTL (`DEFAULT_WEBSOCKET_TOKEN_TTL` in `apps/server/src/auth/SessionStore.ts`). The
handshake verifies the ticket, and each RPC method still enforces its own scope. See
[environment-auth.md](./environment-auth.md).

Hosted pairing is a client-side convenience only. The hosted app must not receive pairing tokens
through query parameters, must not store pairing state server-side, and must not imply that an HTTP
backend is reachable from an HTTPS browser context.

## Version coordination

Remote environments stay online while clients move to newer releases. The environment descriptor
carries the running server version and may advertise a safe replacement path, so the UI can show the
right action without making the transport responsible for process management. The connection
supervisor owns the resulting disconnect and reconnect like any other involuntary close. See
[server-updates.md](./server-updates.md).
Remote servers can outlive several client releases. Clients must use advertised
capabilities and handle their absence, rather than assume their own version
describes the server. Process replacement belongs to the launcher's
[update protocol](./server-updates.md); the connection runtime handles the
resulting disconnect.

## Future work

These remain unbuilt and are listed to keep the model honest:

- third-party tunnel products as additional endpoint providers;
- a relay-hosted OAuth callback broker (see [t3-connect.md](./t3-connect.md));
- richer multi-environment UI beyond the current connections list.

[model]: ../../packages/client-runtime/src/connection/model.ts
[onboarding]: ../../packages/client-runtime/src/connection/onboarding.ts
[authremote]: ../../packages/client-runtime/src/authorization/remote.ts
[sshenv]: ../../apps/desktop/src/ssh/DesktopSshEnvironment.ts
[sshtunnel]: ../../packages/ssh/src/tunnel.ts
