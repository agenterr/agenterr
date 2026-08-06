#!/usr/bin/env bash
# Replays an agent session against Agenterr's MCP tools for the README demo.
# The tool outputs below are captured verbatim from a live instance
# (2026-08-06); the assistant lines are authored. Re-render with vhs:
#   vhs docs/demo/demo.tape

DIM=$'\e[2m'; BOLD=$'\e[1m'; RESET=$'\e[0m'
CYAN=$'\e[36m'; GREEN=$'\e[32m'; RED=$'\e[31m'; MAGENTA=$'\e[35m'

say()  { printf '%s\n' "$1"; }
tool() { sleep 0.9; printf '%s⏺%s %s%s%s\n' "$GREEN" "$RESET" "$BOLD" "$1" "$RESET"; sleep 0.7; }
out()  { printf '%s' "$DIM"; sed 's/^/  ⎿  /' <<< "$1"; printf '%s\n' "$RESET"; }

printf "\e[2J\e[H"; sleep 0.5
say "${MAGENTA}>${RESET} ${BOLD}why is prod failing?${RESET}"
sleep 1.2

tool "agenterr:get_stats()"
out 'logs: 158  events: 74  open issues: 17
2026-08-06: 158 logs, 149 events'

tool "agenterr:list_issues(environment: \"production\", limit: 4)"
out '15 open issues in payment-api-1 (production):
#5 ×9 [ERROR] *net.OpError: redis: dial tcp 10.0.4.11:6379: i/o timeout (last 27m ago, first 27m ago)
#17 ×1 [ERROR] *stripe.Error: stripe: card_declined for cus_28121x (last 27m ago, first 27m ago)
#16 ×1 [ERROR] *stripe.Error: stripe: card_declined for cus_21594x (last 27m ago, first 27m ago)
#15 ×1 [ERROR] *stripe.Error: stripe: card_declined for cus_22382x (last 27m ago, first 27m ago)
(+11 more — refine filters)'

tool "agenterr:get_issue(id: 5)"
out '#5 [ERROR] *net.OpError: redis: dial tcp 10.0.4.11:6379: i/o timeout
status: open  fingerprint: 3efefb31eecb46be
first seen: 2026-08-06T08:16:20Z  last seen: 2026-08-06T08:16:20Z  count: 9
environments: production
7d: 0 0 0 0 0 0 9

newest event (2026-08-06T08:16:20Z):
service: auth-svc
redis: dial tcp 10.0.4.206.169:6379: i/o timeout
exception.type=*net.OpError'

sleep 1.4
say ""
say "${CYAN}auth-svc can't reach redis — 9 i/o timeouts on :6379 in the last half hour,"
say "all in production. The dial errors span several 10.0.4.x addresses, so this"
say "looks like a network/DNS problem, not one bad node. Checking the redis"
say "service config next; I'll resolve issue #5 once the fix ships.${RESET}"
sleep 3
