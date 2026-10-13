# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

A Raspberry Pi-based kiln controller that provides web-enabled temperature control, scheduling, and monitoring for electric kilns. The system uses PID control with support for various thermocouple types (K, J, N, R, S, T, E, B) and runs as a gevent-based web server with WebSocket support for real-time updates.

## Development Commands

### Setup
```bash
# Install dependencies into a uv-managed virtual environment (.venv)
uv sync

# Initial configuration
cp config.py.EXAMPLE config.py
cp -r storage.EXAMPLE storage
```

### Running
```bash
# Start the controller
uv run ./kiln-controller.py

# Access web interface at http://127.0.0.1:8081
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest Test/test_oven.py

# Run tests with verbose output
uv run pytest -v

# Hardware testing scripts (when connected to actual hardware)
uv run ./scripts/test-thermocouple.py
uv run ./scripts/test-output.py
uv run ./scripts/gpioreadall.py
```

### Code Quality
```bash
# Run linter
uv run ruff check .

# Auto-fix issues
uv run ruff check --fix .
```

### Tuning & Utilities
```bash
# PID auto-tuner (calculates optimal PID values using Ziegler-Nichols)
uv run ./scripts/kiln-tuner.py

# With visualization (requires matplotlib from the viz dependency group)
uv sync --group viz
uv run ./scripts/kiln-tuner.py --showplot

# Log firing data to CSV
uv run ./scripts/kiln-logger.py
```

## Architecture

### Core Components

**Main Server** (`kiln-controller.py`)
- Entry point that initializes the entire system
- Bottle/gevent-based web server with WebSocket handler
- Coordinates Oven, OvenWatcher, Scheduler, Tuner, and AlertManager
- Serves static web app from `public/` directory
- Provides REST API at `/api` and WebSocket at `/control`

**Oven** (`lib/oven.py`)
- Main control loop running in its own thread
- Polymorphic: `SimulatedOven` for testing, `RealOven` for hardware
- Contains nested classes:
  - `Output`: GPIO control for solid state relay (SSR)
  - `Board`: Abstract hardware interface
  - `TempSensor`: Temperature reading (threaded, polls every `sensor_time_wait`)
  - `Profile`: Firing schedule with time/temp data points
- PID control with integral wind-up prevention
- Handles power outage recovery by automatically restarting profiles
- Duty cycle is `sensor_time_wait` (default 2s) - controls how often relay decisions are made

**OvenWatcher** (`lib/ovenWatcher.py`)
- Monitors oven state in a separate thread
- Detects abnormal conditions (thermocouple failures, stuck relays)
- Triggers emergency shutoff when needed
- Feeds data to AlertManager

**Scheduler** (`lib/scheduler.py`)
- Manages future kiln runs scheduled to start at specific times
- Persists schedules to disk (survives reboots)
- Polls every `schedule_poll_interval` to check if runs should start
- Supports chaining runs (one starts after another completes)
- Can skip the first part of the profile with `start_at` parameter

**AlertManager & Sinks** (`lib/alerts.py`)
- Centralized alert system for safety and lifecycle events
- `AlertStore`: Persists which alerts are enabled and delivery settings
- `AlertManager`: Coordinates alert detection and delivery
- Three sink types:
  - `LogSink`: Always logs to daemon log
  - `MqttSink`: Publishes to MQTT if enabled
  - `WebhookSink`: POSTs to webhook URL if configured
- Alert registry in `ALERTS` dict defines all possible alerts

**Tuner** (`lib/tuner.py`)
- Implements Ziegler-Nichols open-loop PID tuning
- Heats kiln at full power and records temperature curve
- Calculates optimal P, I, D values from heating characteristics
- Can update `config.py` automatically with new values

### Data Flow

1. **Temperature Control Loop**:
   - `TempSensor` thread reads thermocouple every `sensor_time_wait` seconds
   - Main `Oven` thread calculates PID output
   - `Output` controls SSR based on PID result
   - State pushed to WebSocket clients in real-time

2. **Scheduling Flow**:
   - User schedules run via web UI or API
   - `Scheduler` persists to `schedule_state_file`
   - Polling thread checks if `start_time` has arrived
   - Calls `fire_callback` to start profile on `Oven`

3. **Alert Flow**:
   - `OvenWatcher` or `Oven` detects condition
   - Calls `AlertManager.emit(alert_id, details)`
   - Manager checks if alert is enabled in `AlertStore`
   - Distributes to all sinks (log, MQTT, webhook)

### Configuration System

**config.py**
- All hardware GPIO pin assignments (`gpio_heat`, SPI pins)
- PID tuning parameters (`pid_kp`, `pid_ki`, `pid_kd`)
- Temperature scale (`temp_scale`: 'f' or 'c')
- Simulation mode (`simulate`: True/False)
- File paths for profiles, state, schedules
- Network settings (`listening_ip`, `listening_port`)
- Alert delivery endpoints if using MQTT/webhook

**Important**: `config.py` and `storage/` are gitignored to prevent committing user-specific settings and firing profiles.

### Profile Format

JSON files in `storage/profiles/`:
```json
{
  "type": "profile",
  "name": "cone-6-long-glaze",
  "data": [
    [0, 65],        // [seconds, temperature]
    [600, 200],     // linear interpolation between points
    [7200, 250],
    [25200, 1976]
  ]
}
```

### State Persistence

- **Firing state**: `state.json` - current profile, runtime, target temp
- **Schedules**: `schedule_state_file` (default: storage/schedules.json)
- **Alerts**: `alert_state_file` (default: storage/alerts.json)

All survive reboots and power outages.

## Important Patterns

### Simulation vs. Real Hardware

Toggle with `config.simulate`:
- `True`: Uses `SimulatedOven` with mathematical model (no GPIO)
- `False`: Uses `RealOven` with actual GPIO and thermocouples

Always develop and test new features with `simulate=True` first.

### Threading Model

- Main thread: Bottle web server (gevent)
- `Oven`: Dedicated thread for control loop
- `TempSensor`: Dedicated thread for temperature polling
- `OvenWatcher`: Dedicated thread for monitoring
- `Scheduler`: Polling in the main event loop

Use locks appropriately when sharing state between threads.

### PID Control

Located in `Oven.run()` method:
- Proportional: responds to the current error
- Integral: accumulates error over time (with wind-up prevention)
- Derivative: responds to rate of change
- Output duty cycle controls relay on-time within `sensor_time_wait` window

### API Integration

RESTful endpoints at `/api`:
- POST with JSON commands: `{"cmd":"run", "profile":"name"}`
- Start runs, stop, pause/resume, schedule, configure alerts
- See `docs/api.md` for complete reference

### Hardware Safety

Critical safety features in `OvenWatcher`:
- Detects if relay is stuck on
- Monitors thermocouple health
- Emergency shutoff capability
- All violations trigger alerts and can stop kiln

## Common Workflows

### Adding a New Alert Type

1. Define alert in `ALERTS` dict in `lib/alerts.py`
2. Add detection logic in `OvenWatcher` or `Oven`
3. Call `alert_manager.emit(alert_id, details)`
4. Alert automatically available in web UI config

### Testing Without Hardware

1. Set `simulate = True` in `config.py`
2. Start server: `uv run ./kiln-controller.py`
3. All temperature readings and relay outputs are simulated
4. Web interface fully functional for testing UI changes

### Deploying to Raspberry Pi

1. Enable SPI: `sudo raspi-config` → Interfacing Options → SPI
2. Set `simulate = False` in `config.py`
3. Configure GPIO pins to match your wiring
4. Tune PID values using web-based auto-tuner or `kiln-tuner.py`
5. Optional: Set up autostart with `./start-on-boot`

## Key Files

- `kiln-controller.py`: Main entry point, web server, API routes
- `lib/oven.py`: Core PID control, hardware abstraction, Profile class
- `lib/scheduler.py`: Future run scheduling
- `lib/alerts.py`: Alert system with multiple delivery methods
- `lib/tuner.py`: PID auto-tuning (Ziegler-Nichols)
- `config.py`: All user-configurable settings (not in repo)
- `storage/profiles/*.json`: Firing schedules (not in repo)
