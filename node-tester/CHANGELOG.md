# Changelog

## 1.1.1

- Fix: recovery ladder no longer fights manual node switches. Switching the
  active node manually — via Mihomo's own UI, node-tester's Settings, MQTT
  select/node, or forcing Direct/Reserve ON — now pauses the ladder's
  "climb to best" behaviour instead of reverting the pick on the very next
  poll. Rescue-if-the-current-node-actually-dies always stays active
  regardless of the pause. Returning to automatic selection (Direct/Reserve
  switched OFF, or a new test-driven switch) resumes normal ladder behaviour
- Feature: Reserve fallback is now independently toggleable (Settings →
  "Reserve fallback"), alongside the existing DIRECT fallback toggle. Off =
  skip the reserve step entirely and fall straight to DIRECT when all Main
  nodes are dead/ineligible. Manual switching to Reserve via MQTT/Settings
  still works regardless of this toggle — it only gates the automatic
  fallback cascade

## 1.1.0

- Feature: three-tier node groups — Main / Backup / Reserve
  Backup is now a true quarantine: tested and tracked, but never eligible for
  connection under any circumstance (previously it was just ranked below Main,
  which still allowed it to be picked when nothing else was available)
  Reserve is new: a designated last-resort node used only once every Main node
  is dead or ineligible; falls through to DIRECT if the reserve is also dead
- Feature: minimum grade threshold (Settings → "Minimum grade to stay eligible",
  default C) — a Main node graded below this is treated as dead for selection
  purposes even if it's technically reachable, same effect as Backup
- Feature: continuous recovery ladder — while sitting on anything other than the
  actual top-ranked eligible Main node (DIRECT, Reserve, or a lower-ranked but
  alive Main node), every monitor poll now climbs to the best currently-alive
  candidate instead of waiting for the next scheduled test. Once on the true
  top node, switching goes back to being test-driven only
- Feature: MQTT switch "Reserve node" (mirrors the existing "Direct" switch) —
  ON forces the configured reserve node, OFF returns to best alive Main
- Dashboard: Reserve nodes now shown as their own section, same as Backup

## 1.0.6

- Fix: dead-node auto-switch could fire on a single dropped passive health-check
  Previously, a node classified "uncertain" (not confirmed dead, just missing fresh
  URLTest data) was treated the same as confirmed dead, causing a false-positive
  switch away from a node that was actually still alive
  Now: before switching, do an active ping_filter_nodes() double-check on the
  current node; skip the switch if the ping confirms it's still reachable

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
