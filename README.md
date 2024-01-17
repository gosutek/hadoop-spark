# `run.sh`
## Flags
- `-l` list available scipts
- `-s [1...9]` spark submit script
## Spark submit args
All scripts run with the following args:
- `--deploy-mode`: client
- `--master`: yarn
- `--num-executors`: (varies)

Output will be redirected from stdout to `./output/`. Spark INFO messages are still being displayed in stdout.
