"""python -m oracle <command> <subject> [args...]"""
import sys

from . import verdict, workload

COMMANDS = {
    "ready": workload.ready,
    "setup": workload.setup,
    "leader": workload.leader,
    "write": workload.write,
    "writing": workload.writing,
    "check": verdict.check,
    "dupinfo": verdict.dupinfo,
}

if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
    raise SystemExit(f"usage: python -m oracle <{'|'.join(COMMANDS)}> <subject> [args...]")
COMMANDS[sys.argv[1]](*sys.argv[2:])
