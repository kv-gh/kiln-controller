# Kiln Controller Utility Scripts

This directory contains utility scripts for the kiln controller project.

## Available Scripts

### kiln-tuner.py
PID tuning utility using Ziegler-Nichols open loop method. Helps calculate optimal PID parameters for your kiln.

```bash
uv run python scripts/kiln-tuner.py --help
uv run python scripts/kiln-tuner.py --showplot  # requires matplotlib (see below)
```

### kiln-logger.py
Records firing data to CSV for offline analysis. Connects to the kiln-controller WebSocket.

```bash
uv run python scripts/kiln-logger.py --hostname kiln.local:8081 --csvfile firing.csv
```

### test-output.py
Test your GPIO output relay configuration.

```bash
uv run python scripts/test-output.py
```

### test-thermocouple.py
Test your thermocouple sensor configuration.

```bash
uv run python scripts/test-thermocouple.py
```

### gpioreadall.py
Diagnostic utility to read all GPIO states (Raspberry Pi).

```bash
uv run python scripts/gpioreadall.py
```

## Optional Dependencies

For visualization features (e.g., `kiln-tuner.py --showplot`):

```bash
uv sync --group viz
```

This installs matplotlib for plotting tuning curves.

## Running from the Project Root

All scripts should be run from the project root directory:

```bash
cd /path/to/kiln-controller
uv run python scripts/<script-name>.py [options]
```
