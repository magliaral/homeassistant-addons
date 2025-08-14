from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import os
import tempfile
import time
import yaml
from typing import Any, Dict, List

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# -----------------------------------------------------------
# Grund-Logging
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logging.Formatter.converter = time.localtime
LOG = logging.getLogger("bacnet_hub_addon")

async def _periodic_info_heartbeat(logger: logging.Logger, interval_s: int = 600):
    while True:
        logger.info("heartbeat: service alive")
        try:
            await asyncio.sleep(interval_s)
        except asyncio.CancelledError:
            break

# -----------------------------------------------------------
# Optionen laden
# -----------------------------------------------------------
def load_addon_options() -> Dict[str, Any]:
    try:
        with open("/data/options.json", "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

OPTS = load_addon_options()
BACPYPES_LOG_LEVEL = (OPTS.get("bacpypes_log_level") or "info").lower()
BAC_DEBUG_MODULES: List[str] = OPTS.get("bacpypes_debug_modules") or []  # optional liste

# -----------------------------------------------------------
# Hilfen
# -----------------------------------------------------------
def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as err:
        LOG.warning("Fehler beim Laden von YAML %s: %r", path, err)
        return {}

DEFAULT_CFG_DIR = "/config/bacnet-hub"
DEFAULT_BACPY_YAML_PATH = f"{DEFAULT_CFG_DIR}/bacpypes.yml"

def _bacpypes_dict_to_argv(options: Dict[str, Any]) -> List[str]:
    argv: List[str] = []
    for key, value in (options or {}).items():
        flag = f"--{str(key).replace('_','-')}"
        if isinstance(value, bool):
            if value:
                argv.append(flag)
        elif value is None:
            continue
        elif isinstance(value, (list, tuple)):
            for item in value:
                argv.extend([flag, str(item)])
        else:
            argv.extend([flag, str(value)])
    return argv

def _build_argv_from_config(config: Dict[str, Any]) -> List[str]:
    """
    Erzeuge bacpypes-CLI-Args aus mappings.yaml (Fallback),
    falls keine eigenständige bacpypes.yml vorhanden ist.
    """
    argv: List[str] = []
    bac = config.get("bacpypes", {}) or {}
    if "args" in bac and isinstance(bac["args"], list):
        argv.extend([str(x) for x in bac["args"]])
    elif "options" in bac and isinstance(bac["options"], dict):
        argv.extend(_bacpypes_dict_to_argv(bac["options"]))

    # device fallbacks
    dev = config.get("device", {}) or {}
    if "--device-instance" not in argv and "-i" not in argv and dev.get("device_id"):
        argv.extend(["--device-instance", str(int(dev["device_id"]))])
    if dev.get("address"):
        argv.extend(["--address", str(dev["address"])])
    if dev.get("port"):
        argv.extend(["--port", str(int(dev["port"]))])

    # gewünschte Debug-Module auch via Args (wir füttern Parser doppelt – okay!)
    try:
        from bacpypes3.settings import settings as bp_settings_after
        for mod in bp_settings_after.get("debug", []):
            argv.extend(["--debug", mod])
        if bp_settings_after.get("color") is False:
            argv.extend(["--color", "false"])
        if bp_settings_after.get("route_aware") is False:
            argv.extend(["--route-aware", "false"])
    except Exception:
        pass

    return argv

# -----------------------------------------------------------
# Start – NUR BACpypes-App + Debug sichtbar machen
# -----------------------------------------------------------
async def main():
    # 1) mappings.yaml und ggf. eingebettete bacpypes_yaml lesen
    mappings_path = f"{DEFAULT_CFG_DIR}/mappings.yaml"
    cfg_all = _load_yaml(mappings_path)

    # 2) Parser/Args vorbereiten – YAML bevorzugt
    from importlib import import_module
    YAMLArgumentParser = import_module("bacpypes3.argparse").YAMLArgumentParser
    SimpleArgumentParser = import_module("bacpypes3.argparse").SimpleArgumentParser

    if os.path.exists(DEFAULT_BACPY_YAML_PATH):
        parser = YAMLArgumentParser()
        args = parser.parse_args(["--yaml", DEFAULT_BACPY_YAML_PATH])
        source = DEFAULT_BACPY_YAML_PATH
    elif isinstance(cfg_all.get("bacpypes_yaml"), dict):
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yml") as tf:
            yaml.safe_dump(cfg_all["bacpypes_yaml"], tf)
            temp_path = tf.name
        try:
            parser = YAMLArgumentParser()
            args = parser.parse_args(["--yaml", temp_path])
            source = "embedded:bacpypes_yaml"
        finally:
            try:
                os.remove(temp_path)
            except OSError:
                pass
    else:
        argv = _build_argv_from_config(cfg_all)
        parser = SimpleArgumentParser()
        args = parser.parse_args(argv)
        source = "argv:fallback"

    # 3) Debug-Settings/Args ausgeben (Sichtprüfung)
    try:
        from bacpypes3.settings import settings as bp_settings_now
        LOG.debug("args source: %s", source)
        LOG.debug("args: %r", vars(args))
        LOG.debug("settings: %s", dict(bp_settings_now))
    except Exception:
        LOG.debug("args: %r", vars(args))

    # 4) Application starten (ohne weitere Logik)
    Application = import_module("bacpypes3.app").Application
    app = Application.from_args(args)

    # 5) Kleines Device/Bind-Info-Logging (optional)
    try:
        Address = import_module("bacpypes3.pdu").Address
        bind_addr = getattr(args, "address", None) or "0.0.0.0"
        bind_port = getattr(args, "port", None) or 47808
        LOG.info("BACnet bound to %s:%s", bind_addr, bind_port)
        LOG.debug("ipv4_address: %r", Address(f"{bind_addr}"))
    except Exception:
        pass

    # 6) Heartbeat + Idle
    hb = asyncio.create_task(_periodic_info_heartbeat(LOG, 600))
    try:
        stopper = asyncio.Event()
        await stopper.wait()
    finally:
        hb.cancel()
        close = getattr(app, "close", None)
        if callable(close):
            res = close()
            if inspect.isawaitable(res):
                await res

# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
if __name__ == "__main__":
    # Log-Level des Root/Paket-Loggers passend setzen
    if BACPYPES_LOG_LEVEL == "debug":
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("bacpypes3").setLevel(logging.DEBUG)
        logging.getLogger(__name__).setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
