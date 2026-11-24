#!/bin/sh
# Change S3 edges through Toxiproxy. Arguments: the scenario's short name
# (the Compose project is temper-automq-<name>), the target, the action.
#   target   server1|server2|server3, all, or leader:<partition> for the
#            server that leads that partition now
#   action   cut      refuse new connections and close open ones
#            restore  undo cut and remove every toxic
#            slow <ms>  add <ms> of latency to every response
#            reset    reset each connection as it opens (a failing endpoint)
set -e
p="temper-automq-$1"
target=$2
action=$3
T="docker exec $p-toxiproxy-1 /toxiproxy-cli"
case "$target" in
  all) servers="server1 server2 server3" ;;
  leader:*)
    part=${target#leader:}
    servers=$(docker compose -f temper/compose.yml -p "$p" run --rm --no-deps -T oracle \
      python -m oracle leader x "$part" 2>/dev/null | tail -1)
    [ -n "$servers" ] || { echo "no leader found for partition $part" >&2; exit 1; } ;;
  *) servers=$target ;;
esac
state() { $T list | awk -v n="$1" '$1 == n {print $4}'; }
for s in $servers; do
  proxy="s3_$s"
  case "$action" in
    cut) [ "$(state "$proxy")" = enabled ] && $T toggle "$proxy" >/dev/null ;;
    restore)
      [ "$(state "$proxy")" = disabled ] && $T toggle "$proxy" >/dev/null
      for t in slow reset; do $T toxic remove -n "$t" "$proxy" >/dev/null 2>&1 || true; done ;;
    slow) $T toxic add -n slow -t latency -a latency="$4" "$proxy" >/dev/null ;;
    reset) $T toxic add -n reset -t reset_peer -a timeout=0 "$proxy" >/dev/null ;;
    *) echo "unknown action $action" >&2; exit 2 ;;
  esac
done
echo "s3 edge $action ${4:-} on $servers"
