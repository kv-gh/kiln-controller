#!/usr/bin/env python

import time
import os
import sys
import logging
import json
import datetime
import tarfile
import io
import re
import base64
import subprocess
import importlib
import threading

import requests

import bottle
import gevent
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler
from geventwebsocket import WebSocketError

# try/except removed here on purpose so folks can see why things break
import config

logging.basicConfig(level=config.log_level, format=config.log_format)
log = logging.getLogger("kiln-controller")
log.info("Starting kiln controller")

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, script_dir + '/lib/')
profile_path = config.kiln_profiles_directory

from temp import f_to_c, c_to_f, to_c
from oven import SimulatedOven, RealOven, Profile
from ovenWatcher import OvenWatcher
from scheduler import Scheduler
from tuner import Tuner, DEFAULT_METHOD
from alerts import (AlertStore, AlertManager, LogSink, MqttSink,
                    WebhookSink, ALERTS, validate_delivery)
from mqttout import enabled as mqtt_enabled

app = bottle.Bottle()

public_root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "public")

# alerts are created before the oven so detection code can be attached
# the moment it exists. LogSink records every alert to the daemon log.
# the mqtt and webhook sinks read their settings live from the same
# store as the web ui, so they self-disable until switched on; neither
# can block heater control (mqtt publishes queue on its network thread,
# webhook posts run on throwaway daemon threads).
alert_store = AlertStore()
alert_manager = AlertManager(alert_store)
alert_manager.add_sink(LogSink())
try:
    alert_manager.add_sink(MqttSink(alert_store))
except Exception as e:
    log.error("could not start mqtt alert sink: %s" % e)
try:
    alert_manager.add_sink(WebhookSink(alert_store))
except Exception as e:
    log.error("could not start webhook alert sink: %s" % e)

if config.simulate:
    log.info("this is a simulation")
    oven = SimulatedOven()
else:
    log.info("this is a real kiln")
    oven = RealOven()
ovenWatcher = OvenWatcher(oven)
# this ovenWatcher is used in the oven class for restarts
oven.set_ovenwatcher(ovenWatcher)
oven.set_alert_manager(alert_manager)
tuner = Tuner(oven)

@app.route('/')
def index():
    """
    Handler for the root route ('/') of the application.

    This function serves the 'index.html' file as a static file from the specified
    public root directory.

    Returns:
        HTTPResponse: Static file response containing the 'index.html' file from the
        public root directory.
    """
    return bottle.static_file('index.html', root=public_root)

@app.route('/state')
def state():
    """
    Redirects the user to the details section of the application.

    This function handles GET requests to the '/state' route and
    redirects the client to the '/#details' path, typically used for
    navigating to a specific section in a single-page application.

    Returns:
        bottle.HTTPResponse: A response object initiating a redirect
        to the '/#details' path.
    """
    return bottle.redirect('/#details')

@app.get('/api/stats')
def handle_stats():
    """
    Handles the '/api/stats' endpoint for processing and returning statistics data.

    Summary:
    The '/api/stats' endpoint processes statistical information related to the oven's
    PID controller (if available) and returns the data as a JSON string. If the oven's
    PID or its PID statistics are not available, the function returns None.

    Returns:
        str or None: A JSON-formatted string containing the oven's PID statistics, or
        None if the required data is unavailable.
    """
    log.info("/api/stats command received")
    if hasattr(oven,'pid'):
        if hasattr(oven.pid,'pidstats'):
            return json.dumps(oven.get_display_pidstats())
    return None


@app.post('/api')
def handle_api():
    """
    Handles API requests sent to the '/api' endpoint. This function processes commands related to controlling a kiln,
    including running schedules, scheduling future runs, canceling schedules, listing schedules, and managing the state
    of the oven. Appropriate responses are returned based on the outcome of the requested operations.

    Raises:
        KeyError: If the required key(s) are not present in the incoming JSON request.
        AttributeError: If the oven or its attributes are not correctly configured or initialized.

    Returns:
        dict: A JSON-serializable dictionary containing the success state or details of the requested operation.
    """
    log.info("/api is alive")


    # run a kiln schedule
    if bottle.request.json['cmd'] == 'run':
        wanted = bottle.request.json['profile']
        log.info('api requested run of profile = %s' % wanted)

        # start at a specific minute in the schedule
        # for restarting and skipping over early parts of a schedule
        start_at = 0
        if 'start_at' in bottle.request.json:
            start_at = bottle.request.json['start_at']

        if not start_run(wanted, start_at):
            return { "success" : False, "error" : "profile %s not found" % wanted }

    # schedule a kiln run for a future date and time
    if bottle.request.json['cmd'] == 'schedule':
        return api_schedule(bottle.request.json)

    # cancel a scheduled kiln run
    if bottle.request.json['cmd'] == 'cancel_schedule':
        return api_cancel_schedule(bottle.request.json)

    # list scheduled kiln runs
    if bottle.request.json['cmd'] == 'list_schedules':
        return api_list_schedules()

    if bottle.request.json['cmd'] == 'pause':
        log.info("api pause command received")
        # pausing from any other state would leave the oven thread in a
        # PAUSED state with no profile, which it cannot run
        if oven.state != 'RUNNING':
            return { "success" : False,
                     "error" : f"cannot pause, oven state is {oven.state}"}
        oven.state = 'PAUSED'

    if bottle.request.json['cmd'] == 'resume':
        log.info("api resume command received")
        if oven.state != 'PAUSED':
            return { "success" : False,
                     "error" : f"cannot resume, oven state is {oven.state}"}
        oven.state = 'RUNNING'

    if bottle.request.json['cmd'] == 'stop':
        log.info("api stop command received")
        oven.abort_run()

    if bottle.request.json['cmd'] == 'memo':
        log.info("api memo command received")
        memo = bottle.request.json['memo']
        log.info(f"memo={memo}")

    # get stats during a run
    if bottle.request.json['cmd'] == 'stats':
        log.info("api stats command received")
        if hasattr(oven,'pid'):
            if hasattr(oven.pid,'pidstats'):
                return json.dumps(oven.get_display_pidstats())

    return { "success" : True }

@app.get('/api/dump')
def api_dump():
    """
    Handles the API endpoint for generating a compressed tarball containing configuration,
    state, profile, and log files related to the application.

    This function creates an in-memory gzip-compressed tarball archive containing the
    specified files from the application's configuration, state, profiles, and logs. The
    resulting tarball is returned as an HTTP response for the user to download.

    Returns:
        bottle.HTTPResponse: An HTTP response containing the gzip-compressed tarball as
        a binary file attachment for download.
    """
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode='w:gz') as tar:
        _tar_add_path(tar, 'config.py', config.__file__)
        _tar_add_path(tar, 'state.json', config.automatic_restart_state_file)
        for filename in profile_files():
            _tar_add_path(tar, os.path.join('profiles', filename),
                          os.path.join(profile_path, filename))
        _tar_add_bytes(tar, 'kiln.logs',
                       '\n'.join(gather_log_lines()) + '\n')
    out.seek(0)
    return bottle.HTTPResponse(
        out.getvalue(),
        headers={
            'Content-Type': 'application/gzip',
            'Content-Disposition': 'attachment; filename="kiln-config-dump.tar.gz"',
        })

def profile_files():
    """
    Returns a list of files in the directory specified by the `profile_path` variable.

    This function scans the directory at `profile_path` and collects files within it.
    In case of an error, such as an invalid directory or permission issue, it safely
    handles the exception and returns an empty list.

    Returns:
        List[str]: A list of filenames present in the `profile_path` directory. Returns
        an empty list if an exception occurs.
    """
    try:
        return [f for f in os.listdir(profile_path)
                if os.path.isfile(os.path.join(profile_path, f))]
    except Exception:
        return []

def _tar_add_bytes(tar, arc_name, text):
    """
    add an in-memory text file to a tar archive.
    """
    encoded = text.encode('utf-8', errors='replace')
    info = tarfile.TarInfo(arc_name)
    info.size = len(encoded)
    tar.addfile(info, io.BytesIO(encoded))

def _tar_add_path(tar, arc_name, path):
    """
    Adds the content of a file to a tar archive using the provided archive name.

    This function reads a file from the specified path and adds its content to the
    given tar archive under the specified archive name. If an error occurs while
    attempting to read or add the file, the error is logged.

    Parameters:
    tar: The tarfile.TarFile instance to which the content will be added.
    arc_name: str - The name under which the content will be stored in the tar archive.
    path: str - The file path to the content being added.

    Raises:
    No specific error is raised but logs an error message if the operation fails.
    """
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            _tar_add_bytes(tar, arc_name, f.read())
    except Exception:
        log.error("could not add %s to config dump" % path)

def gather_log_lines():
    """
    Gathers and returns unique log lines from the kiln-controller service logs.

    This function executes a shell command using subprocess to retrieve the
    journal logs associated with the kiln-controller systemd service. The
    logs are decoded, split into individual lines, and unique lines are
    sorted before being returned. Non-decodable characters are replaced.

    Returns:
        list[str]: A sorted list of unique log lines. In case of an
        exception, an empty list is returned.
    """
    try:
        out = subprocess.check_output(
            "timeout 60 journalctl -u kiln-controller --no-pager 2>/dev/null",
            shell=True, stderr=subprocess.DEVNULL, timeout=70)
    except Exception:
        return []
    return sorted(set(out.decode('utf-8', errors='replace').splitlines()))

def find_profile(wanted):
    """
    Find a profile by name.

    This function searches for a specific profile by its name within the
    list of files in the specified profile directory. If the profile is found
    and correctly formatted, it is returned. Otherwise, the function logs any
    errors or malformed profiles and continues the search.

    Arguments:
    wanted (str): The name of the profile to search for.

    Returns:
    dict | None: The profile dictionary if found and valid; otherwise, None.
    """
    try:
        profile_files = os.listdir(profile_path)
    except OSError:
        profile_files = []
    for filename in profile_files:
        path = os.path.join(profile_path, filename)
        try:
            with open(path, 'r') as f:
                profile = json.load(f)
        except Exception as e:
            log.error("skipping unreadable profile %s: %s" % (path, e))
            continue
        if not isinstance(profile, dict):
            log.error("skipping malformed profile %s" % (path))
            continue
        if profile.get('name') == wanted:
            return profile
    return None

def start_run(wanted, start_at=0, allow_seek=None):
    """
    Starts a kiln run with the specified profile and settings.

    This function initiates the execution of a kiln profile by locating the
    specified profile, converting its temperature units to Celsius if needed,
    and managing the related processes required to handle the kiln operation.
    An internal conversion is performed for legacy profiles stored in Fahrenheit.
    The function utilizes several subsystems to ensure the proper execution,
    including profile management, oven operations, and alert notifications.

    Parameters:
        wanted (str): The name of the kiln profile to run.
        start_at (int, optional): The time in minutes to start the kiln
            profile from. Defaults to 0.
        allow_seek (bool, optional): Determines whether the kiln is allowed to
            seek to a specific point in the profile. Defaults to None, where it
            automatically handles seeking based on the start_at value.

    Returns:
        bool: True if the kiln profile was successfully started; False otherwise.
    """
    if allow_seek is None:
        allow_seek = True
        if start_at > 0:
            allow_seek = False

    # get the wanted profile/kiln schedule
    profile = find_profile(wanted)
    if profile is None:
        log.error(f"profile {wanted} not found")
        return False

    # disk profiles tagged temp_units "c" are already internal Celsius.
    # legacy profiles predate temp_units and are stored in Fahrenheit.
    # convert those to Celsius before running, the same way the ui does.
    if profile.get("temp_units") != "c":
        profile = convert_to_c(profile)
        profile["temp_units"] = "c"

    # FIXME juggling of json should happen in the Profile class
    profile_json = json.dumps(profile)
    profile = Profile(profile_json)
    oven.run_profile(profile, startat=start_at, allow_seek=allow_seek)
    ovenWatcher.record(profile)
    alert_manager.emit('run_started',
                       context={'profile': wanted,
                                'startat_minutes': start_at})
    return True

def parse_start_time(value):
    """
    Parses and validates the start time value.

    Summary:
    This function takes a start time value, validates it, and converts it into
    a Unix epoch timestamp if necessary. The start time can either be a Unix epoch
    timestamp (integer or float) or an ISO 8601 formatted string. The function ensures
    that the provided start time is in the future.

    Args:
        value (int | float | str | None): The start time value to parse. It can be
            either a Unix epoch timestamp, an ISO 8601 string, or None.

    Raises:
        ValueError: If the start time is not provided, not in a valid format,
            or not in the future.

    Returns:
        float: The validated and parsed start time as a Unix epoch timestamp.
    """
    if value is None:
        raise ValueError("start_time is required")
    if isinstance(value, (int, float)):
        epoch = float(value)
    else:
        text = str(value).strip()
        try:
            # naive datetimes are treated as local time which is what
            # someone scheduling their own kiln expects
            dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("start_time must be a unix epoch or an ISO 8601 string")
        epoch = dt.timestamp()
    if epoch <= time.time():
        raise ValueError("start_time must be in the future")
    return epoch

def api_schedule(json_body):
    """
    Processes a scheduling request for a given profile and start time, validates the input,
    and interacts with the scheduler to add the profile to the schedule.

    Parameters
    ----------
    json_body : dict
        A dictionary containing the scheduling request. Expected keys are:
        'profile': The profile to be scheduled (required).
        'start_time': Time at which the profile should start, in string format (required).
        'start_at': Optional; offset from start_time in seconds. Defaults to 0 if not provided.
        'chain_after': Optional; specifies a dependency chain for scheduling. Can be one of the
        following formats:
            - 'sched:<schedule_id>' to chain after an existing schedule.
            - 'run:<run_id>' to chain after a specific run.
            Invalid formats will be rejected.

    Returns
    -------
    dict
        A dictionary with the result of the scheduling operation. Possible keys:
        - 'success': A boolean indicating whether the operation succeeded.
        - 'id': The ID of the newly created schedule, present only on success.
        - 'profile': The profile name of the scheduled entry, present only on success.
        - 'start_time': The epoch time for the schedule's start, present only on success.
        - 'error': A string containing the error message, present only on failure.

    Raises
    ------
    KeyError
        If required keys are missing in `json_body`.

    Notes
    -----
    - The function uses the `find_profile` method to ensure the existence of the requested profile.
      An error response is returned if the profile is not found.
    - The given `chain_after` value is validated to ensure it correctly references an existing
      schedule or run. Invalid values result in an error response.
    - `parse_start_time` is utilized to validate and parse the 'start_time' input; any parsing
      issue results in an error response.

    Examples
    --------
    This function does not provide example code inline within the docstring.
    """
    wanted = json_body.get('profile')
    if not wanted:
        return { "success" : False, "error" : "profile is required" }
    if find_profile(wanted) is None:
        return { "success" : False, "error" : "profile %s not found" % wanted }
    try:
        epoch = parse_start_time(json_body.get('start_time'))
    except ValueError as e:
        return { "success" : False, "error" : str(e) }
    start_at = json_body.get('start_at', 0) or 0
    chain_after = json_body.get('chain_after')
    if chain_after:
        if isinstance(chain_after, str) and chain_after.startswith('sched:'):
            anchor = chain_after[len('sched:'):]
            if not any(e['id'] == anchor for e in scheduler.list()):
                return { "success" : False, "error" : f"chain_after schedule {anchor} not found" }
        elif isinstance(chain_after, str) and chain_after.startswith('run:'):
            try:
                int(chain_after[len('run:'):])
            except ValueError:
                return { "success" : False, "error" : f"invalid chain_after {chain_after}" }
        else:
            return { "success" : False, "error" : "invalid chain_after value"}
    entry = scheduler.add(wanted, epoch, startat=start_at, chain_after=chain_after)
    log.info(f"api scheduled profile {wanted} at {epoch}")
    return { "success" : True, "id" : entry["id"], "profile" : wanted, "start_time" : epoch }

def api_cancel_schedule(json_body):
    """
    Cancels a scheduled task identified by its ID.

    This function attempts to cancel a task in the scheduler using the ID provided
    in the input JSON body. If the ID is missing from the input or the task cannot be
    found, an appropriate error message is returned.

    Parameters:
        json_body (dict): A dictionary containing the key 'id' which represents
            the ID of the schedule to be canceled.

    Returns:
        dict: A dictionary with a "success" key indicating whether the cancellation
            was successful. If unsuccessful, an "error" key will describe the issue.
    """
    sid = json_body.get('id')
    if not sid:
        return { "success" : False, "error" : "id is required" }
    if scheduler.cancel(sid):
        return { "success" : True }
    return { "success" : False, "error" : "schedule %s not found" % sid }

def api_list_schedules():
    """
    Provides functionality to list scheduled tasks.

    This function retrieves a list of scheduled tasks from the scheduler
    and returns the information along with a success status.

    Returns:
        dict: A dictionary containing the success status and a list
              of schedules.

    """
    return { "success" : True, "schedules" : scheduler.list() }

def chain_anchor_ready(chain_after):
    """
    Determines if a chain can be triggered based on its anchor condition.

    This function evaluates whether a specific chain anchor is ready to be
    triggered based on predefined conditions. The chain condition can be checked
    against a run sequence or a scheduled entry. The logic handles multiple formats
    of chain anchor conditions and confirms readiness accordingly.

    Parameters:
        chain_after: str
            The chain anchor condition to check. Expected to be in one of the
            following formats:
            - 'run:<sequence_number>' to evaluate against a run sequence.
            - 'sched:<anchor_id>' to evaluate against scheduled entries.

    Returns:
        bool
            True if the anchor condition is met (i.e., the chain can be triggered),
            otherwise False.

    Raises:
        None
    """
    if chain_after.startswith('run:'):
        try:
            return oven.ended_run_sequence >= int(chain_after[len('run:'):])
        except (TypeError, ValueError):
            return False
    if chain_after.startswith('sched:'):
        anchor = chain_after[len('sched:'):]
        for entry in scheduler.list():
            if entry['id'] == anchor:
                return bool(entry.get('fired'))
        return False
    return False

def fire_scheduled_run(entry):
    """
    Fires a scheduled run based on the given entry.

    This function handles the initiation of a firing schedule. It ensures that
    chained schedules are executed only under proper conditions, such as the
    availability of the oven in an idle state and the readiness of the chained
    anchor. Additionally, it starts the oven run using the specified profile
    and the requested start time in the schedule entry.

    Parameters:
    entry (dict): Dictionary containing the details of the schedule to be fired,
                  including the profile, start time, and chaining information.

    Returns:
    bool: Returns True if the schedule was successfully fired; otherwise,
          returns False.

    Raises:
    None
    """
    chain_after = entry.get("chain_after")
    if chain_after:
        # a chained firing starts shortly after the firing it follows
        # really ends, not at its estimated start_time.
        if not chain_anchor_ready(chain_after):
            return False
        if oven.state != "IDLE":
            return False
        if time.time() < oven.idle_since + config.schedule_chain_buffer:
            return False
    elif oven.state != "IDLE":
        log.warning(f"schedule {entry['id']} ({entry['profile']}) skipped, oven state = {oven.state}")
        return False
    start_at = entry.get("start_at", 0) or 0
    # a scheduled run always starts from the beginning (or the requested
    # start_at) regardless of how hot the kiln is from a previous run.
    if not start_run(entry["profile"], start_at, allow_seek=False):
        log.error(f"schedule {entry['id']} could not fire, profile {entry['profile']} not found")
        return False
    log.info(f"schedule {entry['id']} fired, starting profile {entry['profile']}")
    return True

# scheduled runs - fires future firings when their time arrives
scheduler = Scheduler()
scheduler.fire_callback = fire_scheduled_run
scheduler.alert_emit = alert_manager.emit

def reload_config_module():
    """
    Reload the configuration module by removing its cached file and re-importing it.

    The function checks if the configuration module has a cached file. If a cached
    file exists, it attempts to remove it from the file system. Afterward, it reloads
    the configuration module to ensure any changes made to the module are reflected.

    Raises:
        OSError: If there is an issue removing the cached file.
    """
    cached = getattr(config, '__cached__', None)
    if cached:
        try:
            os.remove(cached)
        except OSError:
            pass
    importlib.reload(config)

def save_config(text):
    """
    Saves and updates the configuration file dynamically.

    This function compiles the provided configuration text and writes it to the
    configuration file. If an error occurs during the reload of the configuration
    module, the original configuration is restored.

    Parameters:
    text: str
        The configuration code to be written to the configuration file.

    Raises:
    Exception
        If an error occurs during the execution or reloading of the configuration module,
        the exception is propagated after restoring the original configuration.
    """
    compile(text, 'config.py', 'exec')
    with open(config.__file__, 'r') as f:
        original = f.read()
    with open(config.__file__, 'w') as f:
        f.write(text)
    try:
        reload_config_module()
    except Exception:
        with open(config.__file__, 'w') as f:
            f.write(original)
        reload_config_module()
        raise

@app.get('/api/config/editor')
def api_config_editor():
    """
    Handles the retrieval and return of the contents of the configuration
    file for editor purposes. The endpoint reads the file and serves its
    content as plain text. This can be useful for viewing or editing the
    config directly via the client interface. If the file cannot be read,
    an error response is returned.

    Raises:
        HTTPResponse: A server error (HTTP 500) if the configuration file
        cannot be read successfully.

    Returns:
        HTTPResponse: The plaintext contents of the configuration file sent
        with a 'Content-Type' header of 'text/plain'. For error cases, an
        HTTPResponse with the error message and 500 status is returned.
    """
    try:
        with open(config.__file__, 'r') as f:
            text = f.read()
    except Exception as error:
        log.error(f"could not read config.py: {error}")
        return bottle.HTTPResponse(str(error), status=500)
    return bottle.HTTPResponse(text, headers={'Content-Type': 'text/plain'})

@app.post('/api/config/editor')
def api_config_editor_save():
    """
    Handles the saving and reloading of the configuration file via an HTTP POST request.
    This endpoint processes the incoming JSON request to update the configuration file,
    validates its syntax, schedules process restarts if necessary, and provides feedback
    to the client.

    Raises:
        bottle.HTTPResponse: If the 'config' key is missing from the request body.
        bottle.HTTPResponse: If a syntax error is detected in the provided configuration.
        bottle.HTTPResponse: For any other unexpected errors during the save/reload process.

    Returns:
        dict: A response indicating whether the operation was successful, whether a restart
        was scheduled, and any warnings if applicable.

    Parameters:
        None
    """
    body = bottle.request.json
    if not body or 'config' not in body:
        log.error("config.py save rejected: no config in request")
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "no config in request"}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    try:
        save_config(body['config'])
    except SyntaxError as e:
        log.error("config.py syntax error, save rejected: %s" % e)
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "syntax error: %s" % e}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    except Exception as e:
        log.error("config.py save/reload failed: %s" % e)
        return bottle.HTTPResponse(json.dumps({"success": False, "error": str(e)}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    log.info("config.py saved and reloaded via web ui")
    # a full restart is required for settings only read at startup (PID
    # constants, sensor timing, board selection, simulate flag). if the
    # config reload above succeeded, we know the new config is good, so
    # schedule the restart. any active firing resumes from the automatic
    # restart state file when the process comes back up.
    response = {"success": True, "restart_scheduled": True}
    if oven.state == "RUNNING" and not config.automatic_restarts:
        response["restart_scheduled"] = False
        response["warning"] = ("config saved and reloaded in-process, but the full restart was skipped "
                               "because a firing is active and automatic_restarts is False, so the run "
                               "would be lost. startup-only settings (PID, sensor timing, board) still "
                               "need a restart.")
        log.warning(response["warning"])
    else:
        def _do_restart():
            # give the http response a moment to flush to the browser first
            gevent.sleep(1)
            log.info("restarting process now")
            sys.stdout.flush()
            sys.stderr.flush()
            logging.shutdown()
            os.execv(sys.executable, [sys.executable] + sys.argv)
        gevent.spawn(_do_restart)
        log.info("process restart scheduled")
    return response

@app.get('/api/alerts')
def api_alerts():
    '''return the alert registry with enabled flags, ordered by
    criticality descending, plus the delivery settings, for the config
    tab alerts panel.'''
    return json.dumps({'success': True,
                       'alerts': alert_store.definitions(),
                       'delivery': alert_store.delivery_settings(),
                       'mqtt_configured': mqtt_enabled()})


@app.post('/api/alerts')
def api_alerts_save():
    """
    Handles saving alert configurations including enabling/disabling alerts and updating
    delivery settings. Validates request payload to ensure proper format and data integrity.

    Raises:
        bottle.HTTPResponse: 400 response with error message if the request is malformed,
        contains unknown alert IDs, invalid data types, or fails validation checks.

    Returns:
        dict: A success response containing the updated alerts, delivery settings, and MQTT
        configuration status.
    """
    body = bottle.request.json
    if not isinstance(body, dict) or \
            ('enabled' not in body and 'delivery' not in body):
        log.error("alerts save rejected: nothing to save in request")
        return bottle.HTTPResponse(json.dumps({"success": False,
                                               "error": "nothing to save"}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    updates = body.get('enabled')
    if 'enabled' in body:
        if not isinstance(updates, dict) or not updates:
            log.error("alerts save rejected: no enabled map in request")
            return bottle.HTTPResponse(json.dumps({"success": False,
                                                   "error": "no enabled map in request"}),
                                       status=400,
                                       headers={'Content-Type': 'application/json'})
        known_ids = {a['id'] for a in ALERTS}
        unknown = sorted(k for k in updates if k not in known_ids)
        if unknown:
            log.error("alerts save rejected: unknown ids %s" % unknown)
            return bottle.HTTPResponse(json.dumps({"success": False,
                                                   "error": "unknown alert ids: %s" % ', '.join(unknown)}),
                                       status=400,
                                       headers={'Content-Type': 'application/json'})
    delivery_updates = body.get('delivery', {})
    if not isinstance(delivery_updates, dict):
        log.error("alerts save rejected: delivery must be a map")
        return bottle.HTTPResponse(json.dumps({"success": False,
                                               "error": "delivery must be a map"}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    try:
        clean_delivery, _ = validate_delivery(delivery_updates)
    except ValueError as e:
        log.error("alerts save rejected: %s" % e)
        return bottle.HTTPResponse(json.dumps({"success": False,
                                               "error": str(e)}),
                                   status=400,
                                   headers={'Content-Type': 'application/json'})
    # everything validated; apply it all
    if 'enabled' in body:
        for alert_id, value in updates.items():
            alert_store.set_enabled(alert_id, value)
    if clean_delivery or 'delivery' in body:
        alert_store.set_delivery(clean_delivery)
    return {"success": True,
            "alerts": alert_store.definitions(),
            "delivery": alert_store.delivery_settings(),
            "mqtt_configured": mqtt_enabled()}

@app.post('/api/tune')
def handle_tune():
    """
    Handles API requests related to tuning operations for an oven and its tuner system.

    This function processes requests to start a new tuning operation, obtain
    the current tuning status, or stop the ongoing tuning process. Additionally,
    it validates the input parameters for tuning, ensuring they adhere to
    specified constraints before dispatching tasks to the tuner. Threaded
    execution is used when initiating tuning to allow asynchronous operation.

    Raises:
        - Errors are captured and returned in the response for invalid requests,
          including incorrect command, missing required parameters, and invalid
          parameter values.

    Arguments:
        None

    Returns:
        dict: A dictionary containing the success status of the operation, details
        of any errors encountered, or the current tuning status as appropriate.
    """
    body = bottle.request.json or {}
    cmd = body.get('cmd')

    if cmd == 'start':
        if oven.state not in ("IDLE", "TUNING"):
            return {"success": False,
                    "error": "cannot tune while a schedule is running (state: %s)" % oven.state}
        # only reject while a tuning run is actually in flight; DONE and
        # ERROR are terminal states that must allow starting again
        # (previously anything but IDLE was rejected, which locked the
        # tuner up after its first run)
        if tuner.state in (Tuner.HEATING, Tuner.COOLING, Tuner.CALCULATING):
            return {"success": False, "error": "tuner is already running"}

        target_temp = body.get('target_temp')
        if target_temp is None:
            return {"success": False, "error": "target_temp is required"}

        method = body.get('method', DEFAULT_METHOD)
        tangent_divisor = body.get('tangent_divisor', 8)

        try:
            target_c = to_c(float(target_temp))
        except (ValueError, TypeError):
            return {"success": False, "error": "invalid target_temp"}

        try:
            tangent_divisor = float(tangent_divisor)
        except (ValueError, TypeError):
            return {"success": False, "error": "invalid tangent_divisor"}

        if tangent_divisor < 2:
            return {"success": False, "error": "tangent_divisor must be >= 2"}

        log.info("api tune start: target=%.1f method=%s tangent_divisor=%.0f" %
                 (float(target_temp), method, tangent_divisor))
        threading.Thread(target=tuner.start, args=(target_c, method, tangent_divisor), daemon=True).start()
        return {"success": True}

    elif cmd == 'status':
        return {"success": True, **tuner.get_status()}

    elif cmd == 'stop':
        log.info("api tune stop")
        tuner.stop()
        return {"success": True}

    return {"success": False, "error": "unknown tune command"}

########################################################################
# community profiles - browse, download, and share profiles with the
# shared kiln-profiles github repo (see the settings in config.py).
# downloads are public; sharing forks the repo and opens a pull request
# using the sharer's own github token.

# a small cache for the remote listing so manual browsing doesn't slam
# the github api rate limit (60/hr unauthenticated)
_remote_cache = {"at": 0.0, "data": None}
REMOTE_CACHE_TTL = 60  # seconds


def _repo_owner_repo():
    """
    Extracts and returns the owner and repository name from a specified GitHub repository URL.

    This function retrieves the GitHub repository URL from the `kiln_profiles_repo`
    attribute of the `config` object. It checks if the URL contains "github.com/" and,
    if present, extracts and returns the part following this marker, representing
    the repository owner and name.

    Returns:
        str or None: A string containing the owner and repository name in the
        format "owner/repo". Returns None if the URL does not contain the
        "github.com/" marker or if the extraction results in an empty value.
    """
    url = str(getattr(config, "kiln_profiles_repo", "")).rstrip("/")
    marker = "github.com/"
    if marker not in url:
        return None
    return url.split(marker, 1)[1].strip("/") or None


def _github_headers(token=None):
    """
    Returns a dictionary of headers for GitHub API requests.

    Args:
        token (str, optional): A GitHub personal access token for authentication.

    Returns:
        dict: A dictionary containing the headers for the GitHub API request.
    """
    headers = {"Accept": "application/vnd.github.v3+json", "User-Agent": "kiln-controller"}
    if token:
        headers["Authorization"] = "token %s" % token
    return headers


def _github_api_url(path):
    """
    Returns the GitHub API URL for the given path.

    Args:
        path (str): The path to append to the GitHub API URL.

    Returns:
        str: The complete GitHub API URL.
    """
    return "https://api.github.com/" + path.lstrip("/")


def _raw_url(path):
    """
    Returns the raw URL for the given path in the GitHub repository.

    Args:
        path (str): The path to append to the raw URL.

    Returns:
        str or None: The complete raw URL for the given path, or None if the repository is not configured.
    """
    repo = _repo_owner_repo()
    if not repo:
        return None
    return "https://raw.githubusercontent.com/%s/%s/%s" % (repo, config.kiln_profiles_branch, path)


def list_remote_profiles(force=False):
    """
    Fetches and lists remote profiles with optional caching.

    This function retrieves and processes a list of remote profiles from a configured
    repository URL. Profiles are categorized, tagged, and checked for specific metadata.
    Caching is used to reduce repeated fetch requests within a preset time-to-live (TTL).

    Parameters:
    force: bool, optional
        If set to True, bypasses the cache and forces a fresh retrieval of the profiles.

    Returns:
    dict
        A dictionary containing the result of the operation. When successful, includes a
        "success" key with a value of True, as well as keys for "upload_enabled",
        "categories", "tags", and "profiles". If unsuccessful, contains "success" set to
        False and an "error" key with details.

    Raises:
    Exception
        Propagates exceptions during the retrieval or processing of the profiles in the form
        of error messages logged and included in the error response.
    """
    if not force and _remote_cache["data"] is not None and time.time() - _remote_cache["at"] < REMOTE_CACHE_TTL:
        return _remote_cache["data"]
    index_url = str(getattr(config, "kiln_profiles_index_url", "")).strip()
    if not index_url:
        return {"success": False, "error": "kiln_profiles_index_url not configured"}
    try:
        resp = requests.get(index_url, headers=_github_headers(), timeout=15)
        resp.raise_for_status()
        index = resp.json()
        if not isinstance(index, list):
            return {"success": False, "error": "unexpected response from the kiln-profiles repo"}
        categories, tags, profiles = [], [], []
        seen_cats, seen_tags = set(), set()
        for entry in index:
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            name = str(entry["name"])
            category = str(entry.get("category") or "uncategorized")
            entry_tags = [str(t) for t in (entry.get("tags") or []) if str(t).strip()]
            if category not in seen_cats:
                seen_cats.add(category)
                categories.append(category)
            for tag in entry_tags:
                if tag not in seen_tags:
                    seen_tags.add(tag)
                    tags.append(tag)
            local_name = name + ".json"
            profiles.append({
                "category": category,
                "name": name,
                "path": "%s/%s" % (category, local_name),
                "tags": entry_tags,
                "description": entry.get("description") or "",
                "units": entry.get("units") or "",
                "installed": os.path.isfile(os.path.join(profile_path, local_name)),
            })
    except Exception as e:
        log.error("could not list community profiles: %s" % e)
        return {"success": False, "error": "could not reach the kiln-profiles repo: %s" % e}
    data = {"success": True, "upload_enabled": bool(_repo_owner_repo()),
            "categories": categories, "tags": tags, "profiles": profiles}
    _remote_cache.update({"at": time.time(), "data": data})
    return data


def import_profile(profile):
    """
    Validates and imports a profile, ensuring it conforms to the required structure
    and properly formatted data. Converts temperature units to Celsius if necessary
    and saves the profile as a JSON file in the designated directory.

    Arguments:
        profile (dict): The profile to be imported. Must contain a valid name
            (string) and data (list). If temperature unit is not Celsius, it
            will be converted.

    Raises:
        ValueError: If the profile is not a dictionary, the name is invalid, or
            if the profile lacks the required structure or data.

    Returns:
        str: The file path where the profile JSON is saved.
    """
    if not isinstance(profile, dict):
        raise ValueError("invalid profile")
    if not valid_profile_name(profile.get("name")):
        raise ValueError("invalid profile name")
    if not isinstance(profile.get("data"), list):
        raise ValueError("profile has no data")
    profile["name"] = str(profile["name"])
    if profile.get("temp_units") != "c":
        profile = convert_to_c(profile)
        profile["temp_units"] = "c"
    os.makedirs(profile_path, exist_ok=True)
    filepath = os.path.join(profile_path, profile["name"] + ".json")
    with open(filepath, "w+") as f:
        f.write(json.dumps(profile))
    log.info("imported community profile %s" % profile["name"])
    return filepath


@app.get('/api/profiles/remote')
def api_profiles_remote():
    """
    Returns a list of remote profiles.
    """
    return json.dumps(list_remote_profiles())


@app.post('/api/profiles/remote/import')
def api_profiles_remote_import():
    """
    Handles the import of remote profiles via an API endpoint.

    This function is an HTTP POST handler that processes requests for importing remote
    profile files hosted on GitHub. It performs validation on the file path, fetches
    the profile file if valid, and imports the profile data into the system. The
    function also ensures that invalid inputs or errors during the process are handled
    gracefully with appropriate responses.

    Raises:
        HTTPResponse: If the provided path is invalid, the associated repository is
        incorrectly configured, or the profile cannot be downloaded, an appropriate
        HTTPError with a JSON response is raised.

    Returns:
        json: Returns a JSON response indicating the success or failure of the
        profile import operation.
    """
    body = bottle.request.json or {}
    path = body.get("path") or ""
    if not path or ".." in path or not path.endswith(".json"):
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "invalid profile path"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    if not _repo_owner_repo():
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "kiln_profiles_repo must be a github.com URL"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    try:
        resp = requests.get(_raw_url(path), headers=_github_headers(), timeout=15)
        resp.raise_for_status()
        profile = resp.json()
        import_profile(profile)
    except ValueError as e:
        return bottle.HTTPResponse(json.dumps({"success": False, "error": str(e)}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    except Exception as e:
        log.error("could not import community profile: %s" % e)
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "could not download profile: %s" % e}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    _remote_cache["data"] = None  # installed state changed
    return json.dumps({"success": True, "name": profile["name"]})


@app.post('/api/profiles/remote/upload')
def api_profiles_remote_upload():
    """
    Handles the API endpoint for uploading and sharing a profile to a remote repository via a GitHub pull request.

    This API function validates the incoming request, processes a specified profile, and submits it as a pull
    request to the configured repository. It requires a valid GitHub token, profile data, and category. Profiles are
    normalized locally before being shared.

    Returns detailed information about the submission, including the created pull request URL and number, if successful.

    Raises HTTP responses with relevant error messages if any part of the request validation or processing fails.

    Parameters:
        None

    Returns:
        str: A JSON-encoded string containing success status and additional information about the processed profile
             if successful. If an error occurs, it returns a JSON-encoded string with success=False and an error message.
    """
    body = bottle.request.json or {}
    profile = body.get("profile") or {}
    category = str(body.get("category") or "pottery")
    token = str(body.get("github_token") or "").strip()
    if not token:
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "sharing is disabled (no github token provided)"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    if not isinstance(profile, dict) or not profile.get("name"):
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "no profile in request"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    name = str(profile["name"])
    if not valid_profile_name(name):
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "invalid profile name"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    if not re.match(r"^[A-Za-z0-9._-]+$", category):
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "invalid category"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    if not _repo_owner_repo():
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "kiln_profiles_repo must be a github.com URL"}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    try:
        # save locally first (normalizes to celsius), then share
        import_profile(profile)
    except ValueError as e:
        return bottle.HTTPResponse(json.dumps({"success": False, "error": str(e)}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    try:
        pr_url, pr_number = share_profile_as_pr(profile, category, token)
    except Exception as e:
        log.error("could not submit community profile: %s" % e)
        return bottle.HTTPResponse(json.dumps({"success": False, "error": "share failed: %s" % e}),
                                   status=400,
                                   headers={"Content-Type": "application/json"})
    log.info("shared profile %s to %s as pull request #%s" % (name, category, pr_number))
    _remote_cache["data"] = None  # local installed state changed
    return json.dumps({"success": True, "name": name, "category": category,
                       "pr_url": pr_url, "pr_number": pr_number})


def _ensure_fork(owner_repo, token):
    """
    Ensures that a fork of a specified GitHub repository exists for the authenticated user.
    If the fork does not exist, it creates one and ensures it is properly synchronized.
    This includes updating branches to match the upstream repository using a best-effort
    method.

    Parameters:
        owner_repo (str): The repository to fork, specified in the format 'owner/repo'.
        token (str): The user's personal GitHub access token for API authentication.

    Returns:
        str: The repository name of the user's fork, in the format 'username/repo'.
    """
    user = requests.get(_github_api_url("user"), headers=_github_headers(token), timeout=15)
    user.raise_for_status()
    username = user.json()["login"]
    fork_repo = "%s/%s" % (username, owner_repo.split("/", 1)[1])
    if requests.get(_github_api_url("repos/%s" % fork_repo), headers=_github_headers(token),
                    timeout=15).status_code == 404:
        requests.post(_github_api_url("repos/%s/forks" % owner_repo),
                      json={"default_branch_only": True},
                      headers=_github_headers(token), timeout=15)
        # forking is asynchronous; poll until the fork is ready
        deadline = time.time() + 60
        while time.time() < deadline:
            # yield to the gevent hub instead of blocking it: this runs in
            # a request greenlet and gevent is not monkey-patched here
            gevent.sleep(2)
            if requests.get(_github_api_url("repos/%s" % fork_repo), headers=_github_headers(token),
                            timeout=15).status_code == 200:
                break
        else:
            raise RuntimeError("your fork did not finish syncing, try again in a moment")
    # best-effort: bring the fork's branch up to date with the upstream
    try:
        requests.post(_github_api_url("repos/%s/merge-upstream" % fork_repo),
                      json={"branch": config.kiln_profiles_branch},
                      headers=_github_headers(token), timeout=15)
    except Exception:
        pass
    return fork_repo


def share_profile_as_pr(profile, category, token):
    """
    Shares the specified profile as a pull request on a GitHub repository.

    This function automates the process of creating a new GitHub branch, uploading the
    given profile data, and opening a pull request to merge the changes into the target
    repository.

    Raises an error if any of the HTTP calls fail or cannot be completed.

    Args:
        profile (dict): The profile data to be shared, formatted as a dictionary.
        category (str): The category under which the profile is to be stored.
        token (str): The GitHub personal access token used for authentication.

    Returns:
        tuple: A tuple containing the HTML URL of the created pull request (str) and its
        pull request number (int).
    """
    name = profile["name"]
    owner_repo = _repo_owner_repo()
    branch = config.kiln_profiles_branch
    fork_repo = _ensure_fork(owner_repo, token)
    username = fork_repo.split("/", 1)[0]

    refs = requests.get(_github_api_url("repos/%s/git/ref/heads/%s" % (fork_repo, branch)),
                        headers=_github_headers(token), timeout=15)
    refs.raise_for_status()
    feature_branch = "kiln-share-%s" % re.sub(r"[^A-Za-z0-9_.-]", "-", name)
    requests.post(_github_api_url("repos/%s/git/refs" % fork_repo),
                  json={"ref": "refs/heads/%s" % feature_branch, "sha": refs.json()["object"]["sha"]},
                  headers=_github_headers(token), timeout=15)

    requests.put(_github_api_url("repos/%s/contents/%s/%s.json" % (fork_repo, category, name)),
                 json={
                     "message": "share %s" % name,
                     "content": base64.b64encode(json.dumps(profile).encode("utf-8")).decode("ascii"),
                     "branch": feature_branch,
                 },
                 headers=_github_headers(token), timeout=20)

    pr = requests.post(_github_api_url("repos/%s/pulls" % owner_repo),
                       json={
                           "title": "Add schedule %s (%s)" % (name, category),
                           "head": "%s:%s" % (username, feature_branch),
                           "base": branch,
                           "body": profile.get("description") or "Community kiln schedule.",
                       },
                       headers=_github_headers(token), timeout=20)
    pr.raise_for_status()
    pr_json = pr.json()
    return pr_json.get("html_url"), pr_json.get("number")

@app.route('/<filename:path>')
def send_static(filename):
    """
    Handles the routing and serving of static files requested by clients from the public
    directory. Logs the requested file name and provides the corresponding file based
    on the specified path.

    Arguments:
        filename (str): The requested file's path and name.

    Returns:
        HTTPResponse: A static file response object that serves the requested file from
        the server's public root directory.
    """
    log.debug("serving %s" % filename)
    return bottle.static_file(filename, root=public_root)


def get_websocket_from_request():
    """
    Gets the WebSocket object from the incoming Bottle request.

    This function retrieves the WebSocket connection associated with the current request from
    the WSGI environment. If the request is not a valid WebSocket request, it terminates the
    request with an HTTP 400 status.

    Raises:
        HTTPResponse: If the request is not a WebSocket request, an HTTP 400 response is raised
        with an appropriate message.

    Returns:
        WebSocket: The WebSocket object extracted from the request.
    """
    env = bottle.request.environ
    wsock = env.get('wsgi.websocket')
    if not wsock:
        bottle.abort(400, 'Expected WebSocket request.')
    return wsock


@app.route('/control')
def handle_control():
    """
    Handles the control WebSocket connection for receiving and processing commands.

    This function manages incoming WebSocket connections and processes
    commands sent from the client to control oven operations such as running a
    profile, simulating a run, or stopping a run. The WebSocket connection
    remains open until an error occurs or the client disconnects.

    Returns:
        None

    Raises:
        WebSocketError: Raised when an issue occurs with the WebSocket
        connection.
    """
    web_socket = get_websocket_from_request()
    log.info("websocket (control) opened")
    while True:
        try:
            message = web_socket.receive()
            if message:
                log.info("Received (control): %s" % message)
                try:
                    msgdict = json.loads(message)
                except ValueError as e:
                    # one bad message must not kill the control greenlet
                    log.error("ignoring malformed message: %s" % (e))
                    msgdict = {}
                if msgdict.get("cmd") == "RUN":
                    log.info("RUN command received")
                    profile_obj = msgdict.get('profile')
                    if not profile_obj:
                        # previously this fell through and raised
                        # NameError on the undefined profile variable,
                        # silently killing the websocket
                        log.error("RUN command without a profile, ignoring")
                    else:
                        try:
                            # the profile comes in display scale from the ui,
                            # store/use it internally in celsius
                            profile = Profile(json.dumps(add_temp_units(profile_obj)))
                            oven.run_profile(profile)
                            ovenWatcher.record(profile)
                        except Exception as e:
                            log.error("could not start run from control socket: %s" % (e))
                elif msgdict.get("cmd") == "SIMULATE":
                    log.info("SIMULATE command received")
                elif msgdict.get("cmd") == "STOP":
                    log.info("Stop command received")
                    oven.abort_run()
            gevent.sleep(1)
        except WebSocketError as e:
            log.error(e)
            break
    log.info("websocket (control) closed")


@app.route('/storage')
def handle_storage():
    """
    Handles WebSocket interactions for storage-related operations.

    This function manages WebSocket connections for handling storage requests such
    as retrieving profiles, deleting profiles, or updating profiles. The WebSocket
    remains open to continuously process incoming messages until an error occurs or
    the client disconnects.

    Raises:
        WebSocketError: If there is an issue with the WebSocket connection.

    Parameters:
        None

    Returns:
        None
    """
    web_socket = get_websocket_from_request()
    log.info("websocket (storage) opened")
    while True:
        try:
            message = web_socket.receive()
            if not message:
                break
            log.debug("websocket (storage) received: %s" % message)

            try:
                msg_dict = json.loads(message)
            except (ValueError, TypeError):
                msg_dict = {}

            if message == "GET":
                log.info("GET command received")
                web_socket.send(get_profiles())
            elif msg_dict.get("cmd") == "DELETE":
                log.info("DELETE command received")
                profile_obj = msg_dict.get('profile')
                if delete_profile(profile_obj):
                    msg_dict["resp"] = "OK"
                else:
                    msg_dict["resp"] = "FAIL"
                web_socket.send(json.dumps(msg_dict))
            elif msg_dict.get("cmd") == "PUT":
                log.info("PUT command received")
                profile_obj = msg_dict.get('profile')
                force = True
                if profile_obj:
                    if save_profile(profile_obj, force):
                        msg_dict["resp"] = "OK"
                    else:
                        msg_dict["resp"] = "FAIL"
                    log.debug("websocket (storage) sent: %s" % message)

                    web_socket.send(json.dumps(msg_dict))
                    web_socket.send(get_profiles())
            gevent.sleep(1)
        except WebSocketError:
            break
    log.info("websocket (storage) closed")


@app.route('/config')
def handle_config():
    """
    Handle the '/config' route.

    This function manages a WebSocket connection for the '/config' endpoint. When a WebSocket
    connection is opened, the server continuously sends the current configuration to the client
    at regular intervals. The WebSocket remains open until an error occurs or the connection is
    closed by the client. The function ensures logging takes place when the WebSocket is opened
    and closed.

    Raises:
        WebSocketError: If an error occurs during the WebSocket communication, leading to the
        termination of the WebSocket connection.
    """
    web_socket = get_websocket_from_request()
    log.info("websocket (config) opened")
    while True:
        try:
            web_socket.receive()
            web_socket.send(get_config())
        except WebSocketError:
            break
        gevent.sleep(1)
    log.info("websocket (config) closed")


@app.route('/status')
def handle_status():
    """
    Handles a WebSocket connection for status updates.

    The function establishes a WebSocket connection, registers the socket as
    an observer to the ovenWatcher, and listens for incoming messages. Received
    messages are echoed back to the client with a confirmation message. The
    connection runs in a loop until an error occurs or the connection is closed.

    Parameters:
        None

    Returns:
        None

    Raises:
        WebSocketError: Raised if an error occurs during WebSocket communication.
    """
    web_socket = get_websocket_from_request()
    ovenWatcher.add_observer(web_socket)
    log.info("websocket (status) opened")
    while True:
        try:
            message = web_socket.receive()
            web_socket.send("Your message was: %r" % message)
        except WebSocketError:
            break
        gevent.sleep(1)
    log.info("websocket (status) closed")


def valid_profile_name(name):
    """
    Validates a profile name based on specific criteria.

    The function checks if the provided name is a non-empty string and
    matches the pattern of allowable characters for a profile name.
    Valid profile names consist of alphanumeric characters, underscores,
    periods, and hyphens.

    Args:
        name (str): The profile name to validate.

    Returns:
        bool: True if the profile name is valid, otherwise False.
    """
    if not name or not isinstance(name, str):
        return False
    return bool(re.match(r"^[A-Za-z0-9._-]+$", name))


def get_profiles():
    """
    Retrieves and processes user profiles from the specified directory.

    This function fetches user profile data stored in JSON files within a predefined
    profile directory. It attempts to read, parse, and validate each profile file,
    skipping those that are unreadable or malformed. Valid profiles are normalized
    for temperature units and sorted alphabetically by the "name" field before being
    returned as a JSON string.

    Returns:
        str: A JSON-formatted string containing a list of processed and sorted user
        profiles. Each profile is represented as a dictionary.

    Raises:
        KeyError: Propagates exceptions related to missing keys during processing.
    """
    try:
        profile_files = os.listdir(profile_path)
    except OSError:
        profile_files = []
    profiles = []
    for filename in profile_files:
        path = os.path.join(profile_path, filename)
        try:
            with open(path, 'r') as f:
                profile = json.load(f)
        except Exception as e:
            log.error("skipping unreadable profile %s: %s" % (path, e))
            continue
        if not isinstance(profile, dict):
            log.error("skipping malformed profile %s" % (path))
            continue
        profiles.append(profile)
    profiles = normalize_temp_units(profiles)
    return json.dumps(sorted(profiles, key=lambda x: x.get("name", "")))


def save_profile(profile, force=False):
    """
    Saves a given profile as a JSON file in a specified directory if certain conditions
    are met. The function performs checks on the profile's validity and handles situations
    where a file with the same name already exists. Optionally allows overwriting
    existing files based on the `force` flag.

    Parameters:
    profile (dict): The profile to be saved. It must be a dictionary containing at
    least the 'name' key with a valid profile name.
    force (bool, optional): A flag to indicate whether to overwrite the existing file if
    one already exists. Defaults to False.

    Returns:
    bool: True if the profile is successfully saved, False otherwise.
    """
    if not isinstance(profile, dict) or not valid_profile_name(profile.get('name')):
        log.error("refusing to save profile with unsafe name %r" % (profile.get('name') if isinstance(profile, dict) else profile,))
        return False
    profile=add_temp_units(profile)
    profile_json = json.dumps(profile)
    filename = profile['name']+".json"
    filepath = os.path.join(profile_path, filename)
    if not force and os.path.exists(filepath):
        log.error("Could not write, %s already exists" % filepath)
        return False
    with open(filepath, 'w+') as f:
        f.write(profile_json)
        f.close()
    log.info("Wrote %s" % filepath)
    _remote_cache["data"] = None  # community "installed" state changed
    return True

def add_temp_units(profile):
    """
    Adds temperature units to a given profile and converts temperature if necessary.

    This function modifies the input profile by adding a 'temp_units' field. The field is
    set to 'c' by default. If the global temperature scale is set to 'f', the function
    invokes a conversion to Celsius.

    Args:
        profile (dict): A dictionary representing the profile to which temperature units
            will be added.

    Returns:
        dict: The updated profile with the 'temp_units' field added and temperature
        converted if required.
    """
    profile['temp_units'] = "c"
    if config.temp_scale == "f":
        profile = convert_to_c(profile)
    return profile

def convert_to_c(profile):
    """
    Converts temperature data within a profile from Fahrenheit to Celsius.

    This function modifies the input profile dictionary by converting all temperature
    values in the "data" key from Fahrenheit to Celsius using the `f_to_c` function.

    Args:
        profile (dict): A dictionary containing profile data. The "data" key
            is expected to hold a list of tuples, where each tuple contains
            time in seconds as the first element and temperature in Fahrenheit
            as the second element.

    Returns:
        dict: The modified profile dictionary with temperatures in Celsius.
    """
    profile["data"] = [(secs, f_to_c(temp)) for (secs, temp) in profile["data"]]
    return profile

def convert_to_f(profile):
    """
    Converts temperature data from Celsius to Fahrenheit in the given profile.

    This function processes a temperature profile stored as a dictionary, converting
    all temperature values from Celsius to Fahrenheit. The profile is expected to
    contain a list of data points where each data point consists of a time in seconds
    and the corresponding temperature in Celsius. The function modifies the profile in
    place and returns it with the converted temperature values.

    Args:
        profile (dict): A dictionary containing a "data" key. The value associated
            with "data" should be a list of tuples, where each tuple contains:
            - secs (int): Time in seconds.
            - temp (float): Temperature value in Celsius.

    Returns:
        dict: The modified profile dictionary with all temperature values converted
        to Fahrenheit.
    """
    profile["data"] = [(secs, c_to_f(temp)) for (secs, temp) in profile["data"]]
    return profile

def normalize_temp_units(profiles):
    """
    Normalize temperature units in the provided profiles based on the system configuration.

    This function processes a list of profiles, each potentially containing temperature information in
    different units. The function ensures that all profiles are normalized to the temperature scale
    defined in the system's configuration settings.

    Parameters:
    profiles: list
        A list of profile dictionaries, where each profile may contain a key `"temp_units"`
        specifying the temperature unit ("c" for Celsius or "f" for Fahrenheit) and associated temperature
        values.

    Returns:
    list
        A list of profile dictionaries with temperature units normalized to match the system's
        configuration.

    """
    normalized = []
    for profile in profiles:
        stored_units = profile.get("temp_units")
        if stored_units == "c" and config.temp_scale == "f":
            profile = convert_to_f(profile)
            profile["temp_units"] = "f"
        elif stored_units == "f" and config.temp_scale == "c":
            profile = convert_to_c(profile)
            profile["temp_units"] = "c"
        normalized.append(profile)
    return normalized

def delete_profile(profile):
    """
    Deletes the specified user profile from the system.

    This function removes the profile JSON file associated with the provided profile
    dictionary. It validates the profile name for safety before proceeding with the
    deletion. Upon successful deletion, the function logs the event and clears the
    data in the remote cache to reflect changes in the community "installed" state.

    Parameters:
    profile (dict): The profile dictionary containing at least a 'name' key, whose
                    value represents the name of the profile to be deleted.

    Returns:
    bool: True if the profile was successfully deleted, otherwise False.
    """
    if not isinstance(profile, dict) or not valid_profile_name(profile.get('name')):
        log.error("refusing to delete profile with unsafe name %r" % (profile.get('name') if isinstance(profile, dict) else profile,))
        return False
    filename = profile['name']+".json"
    filepath = os.path.join(profile_path, filename)
    os.remove(filepath)
    log.info("Deleted %s" % filepath)
    _remote_cache["data"] = None  # community "installed" state changed
    return True

def get_config():
    """
    Retrieve the current configuration settings as a JSON string.

    This function extracts various configuration settings and returns them
    in a JSON-formatted string. The returned configuration includes values
    pertaining to simulation, scaling, rates, currency, and sharing options.
    Private or internal attributes are excluded from the configuration.

    Returns:
        str: A JSON-formatted string containing the current configuration
        settings.

    """
    return json.dumps({"simulate": config.simulate,
        "temp_scale": config.temp_scale,
        "time_scale_slope": config.time_scale_slope,
        "time_scale_profile": config.time_scale_profile,
        "kwh_rate": config.kwh_rate,
        "currency_type": config.currency_type,
        "github_sharing_enabled": _repo_owner_repo() is not None})

def main():
    """
    Main entry point for the application.

    This function initializes the server configuration, sets up required
    background processes, and starts the WSGI server for handling requests.

    Functions:
        schedule_tick(): Recursively schedules the periodic execution
                         of due tasks using the scheduler.

    Attributes:
        ip (str): The IP address the server listens on.
        port (int): The port number the server listens on.

    Arguments:
        None

    Returns:
        None
    """
    ip = "0.0.0.0"
    port = config.listening_port
    log.info("listening on %s:%d" % (ip, port))
    alert_manager.emit('controller_restarted',
                       context={'simulate': bool(config.simulate),
                                'port': port})

    # run the scheduled runs background loop. this is a gevent greenlet
    # that yields to the hub, so it must NOT use blocking time.sleep.
    # gevent is not monkey-patched in this project.
    def schedule_tick():
        scheduler.fire_due()
        gevent.spawn_later(config.schedule_poll_interval, schedule_tick)

    gevent.spawn_later(config.schedule_poll_interval, schedule_tick)

    server = WSGIServer((ip, port), app,
                        handler_class=WebSocketHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
