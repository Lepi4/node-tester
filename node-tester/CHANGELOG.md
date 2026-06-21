# Changelog

## 1.0.5

- Fix: active node MQTT sensor now updates on every monitor poll, not only on auto-switch
  Previously, if user changed node directly in Mihomo UI, MQTT showed stale value forever
  Now: get_active_leaf_node() called every poll, publishes only when value changes

## 1.0.4

- Fix: after Stop, MQTT/scheduler could not restart test ("already running" stuck)
  Root cause: stream loop didn't detect finalize_task.done() when stop was pressed,
  so _running flag was never cleared

## 1.0.2

- Fix 404 after saving settings when running via HA ingress
- Fix all server-side redirects to preserve HA ingress path prefix

## 1.0.1

- Add HA ingress support (Open Web UI button in addon card)
- Fix navigation links to work behind HA reverse proxy
- Auto-patch JS fetch/EventSource to prepend ingress path

## 1.0.0

- Initial release
- Quick, Speed, WebSocket, Browser, Video, DPI tests
- Deep combined test with scoring
- Mihomo/Clash proxy group support
- Scheduled auto-testing
- MQTT result publishing
