# POSIX Semantics

Hyperfile intents to maxium follow POSIX semantics, this doc describe behaviors of Hyperfile in some cases.

## Sync Mode

In general:

- `O_DIRECT` bypass inernal data blocks cache
- `O_SYNC` and `O_DSYNC` behave the same

### WAL Disabled

| Flag | Data blocks cache size | Immediate flush after write |
| ---- | ---- | ---- |
| O_DIRECT | 0 | True |
| O_SYNC | from config | True |
| O_DSYNC | from config | True |

### WAL Enabled

| Flag | Data blocks cache size | Immediate flush after write | Persistent new written data in WAL |
| ---- | ---- | ---- | ---- |
| O_DIRECT | 0 | False | True |
| O_SYNC | from config | False | True |
| O_DSYNC | from config | False | True |
