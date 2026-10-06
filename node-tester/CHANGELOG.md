# Changelog

## 1.1.8

- Add "Groups as nodes" setting: name Mihomo groups (e.g. a Fallback of 4G
  nodes nested inside the main Selector) that should be treated as ONE node.
  Previously every nested group was dropped from the node list, so such
  groups -- and the nodes inside them -- were invisible and could not be
  tagged "reserve". A group-node is listed, health-checked and switched like a
  normal node (new ones default to the "reserve" tier); Mihomo itself picks the
  working member. The active-node resolver stops at a declared group-node
  instead of descending into it.

## 1.1.7

- Fix: even with no manual pin active, every addon/HA restart made the poll
  loop immediately climb to whatever node scored best at that moment --
  looking like "last used node" was being ignored even though nothing was
  ever pinned. The first poll after a process start now just re-establishes
  the previously-active node as the baseline instead of optimizing right
  away; normal auto-climbing (if `auto_node_mode` is on) resumes from the
  second poll onward. Dead-node rescue is unaffected -- it still applies
  immediately regardless of this grace period.

## 1.1.6

- Fix: a manually-pinned node (or `manual_override` pause on the ladder-climb
  behaviour) did not survive a container restart -- `monitor._cache` was
  in-memory only, so `manual_override` reset to `False` on every restart and
  the poll loop would immediately climb away from the pinned node to
  whatever currently scored best. `ladder_expected_node`/`manual_override`
  are now persisted to `data/monitor_state.json` and restored on startup.

## 1.1.5

- Add `panel_admin: false` so the ingress sidebar panel is visible to
  non-admin Home Assistant users too, not just admins.

## 1.1.4

- Fix: a manually-pinned node in a "select"-type proxy group (never gets
  Mihomo's periodic passive URLTest probing the way "urltest"/"fallback"
  group members do) could show up with `alive=False` and no recent history,
  landing straight in `confirmed_dead` -- skipping the active ping double-
  check that only ran for "uncertain" nodes. This incorrectly triggered the
  "rescue from dead" path (which also clears the manual pin) on the very
  next poll, ~monitor_interval_min minutes after switching. The ping
  double-check now also covers `confirmed_dead`, not just `uncertain`.

## 1.1.3

- Fix: a manual node pin was still getting reverted -- by a completed test,
  not the reactive poll loop. `apply_best_node()` (runs after every Quick/
  Deep test when `auto_node_mode` is not "off") switched to the best-ranked
  node and cleared the manual-pause flag unconditionally, regardless of a
  just-made manual pick. It now respects the same pause as the ladder: a
  manually-pinned node that's still alive is left alone; a dead one still
  gets rescued normally.

## 1.1.2

- Fix: MQTT "top N" sensors (`top/1`..`top/10`) no longer rank a Reserve-tagged
  node alongside Main nodes by score alone. A fast Reserve node (e.g. a 4G
  fallback) could previously outrank slower Main nodes and show up at #1,
  which is misleading -- Reserve is a last-resort tier, not a normal
  candidate. Ranking is now strictly tiered: Main before Reserve before
  Backup, score only breaks ties within a tier.

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
