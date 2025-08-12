from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import os
import yaml
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------
# Grund-Logging
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger("bacnet_hub_addon")

# -----------------------------------------------------------
# Add-on-Optionen
# -----------------------------------------------------------
def load_addon_options() -> Dict[str, Any]:
    try:
        with open("/data/options.json", "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

OPTS = load_addon_options()
HA_URL = os.getenv("HA_WS_URL", OPTS.get("ha_url") or "ws://supervisor/core/websocket")
LLAT = (OPTS.get("long_lived_token") or "").strip() or None
BACPYPES_LOG_LEVEL = (OPTS.get("bacpypes_log_level") or "info").lower()

# -----------------------------------------------------------
# bacpypes3 Debugging (ModuleLogger + Decorator)
# -----------------------------------------------------------
try:
    from bacpypes3.debugging import bacpypes_debugging, ModuleLogger  # type: ignore
except Exception:
    def bacpypes_debugging(cls):  # no-op
        return cls
    class ModuleLogger:
        def __init__(self, _): pass
        def __getattr__(self, _): return lambda *a, **k: None

_debug = 1 if BACPYPES_LOG_LEVEL == "debug" else 0
_log = ModuleLogger(globals())

def configure_bacpypes_debug(level_name: str):
    level_map = {
        "debug": logging.DEBUG, "info": logging.INFO,
        "warning": logging.WARNING, "error": logging.ERROR,
    }
    level = level_map.get((level_name or "info").lower(), logging.INFO)

    # ModuleLogger-Ausgaben sichtbar machen
    if level == logging.DEBUG:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger("__main__").setLevel(logging.DEBUG)
        logging.getLogger(__name__).setLevel(logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    logging.getLogger("bacpypes3").setLevel(level)
    for name in (
        "bacpypes3.app","bacpypes3.comm","bacpypes3.pdu","bacpypes3.apdu",
        "bacpypes3.netservice","bacpypes3.service","bacpypes3.local",
    ):
        logging.getLogger(name).setLevel(level)

    LOG.info("bacpypes3 logger level set to '%s'", level_name)

configure_bacpypes_debug(BACPYPES_LOG_LEVEL)

# -----------------------------------------------------------
# Helpers
# -----------------------------------------------------------
async def maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x

def dump_obj_debug(prefix: str, obj) -> None:
    try:
        buf = io.StringIO()
        if hasattr(obj, "debug_contents"):
            obj.debug_contents(file=buf)  # type: ignore[attr-defined]
            LOG.debug("%s\n%s", prefix, buf.getvalue().rstrip())
        else:
            LOG.debug("%s %r (no debug_contents)", prefix, obj)
    except Exception as exc:
        LOG.debug("%s dump failed: %s", prefix, exc)

# -----------------------------------------------------------
# YAML laden und argv für BACpypes bauen
# -----------------------------------------------------------
DEFAULT_CONFIG_PATH = "/config/bacnet-hub/mappings.yaml"

def _load_yaml_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        _log.warning("Konfigurationsdatei nicht gefunden: %s", path)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if _debug:
                _log.debug("Geladene YAML: %r", data)
            return data
    except Exception as err:
        _log.error("Fehler beim Laden von YAML %s: %r", path, err)
        return {}

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

def _build_argv_from_yaml(config: Dict[str, Any]) -> List[str]:
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

    if _debug:
        _log.debug("Finale argv aus YAML: %r", argv)
    return argv

# -----------------------------------------------------------
# Home Assistant WebSocket Client
# -----------------------------------------------------------
class HAWS:
    def __init__(self):
        self.url = HA_URL
        self.token = (
            os.getenv("SUPERVISOR_TOKEN")
            or os.getenv("HASSIO_TOKEN")
            or os.getenv("HOME_ASSISTANT_TOKEN")
            or os.getenv("HOMEASSISTANT_TOKEN")
            or LLAT
        )
        if not self.token:
            raise RuntimeError(
                "No token found. Enable homeassistant_api in add-on config "
                "or set a long_lived_token option."
            )
        self.ws = None
        self._id = 1
        self.state_cache: Dict[str, Dict[str, Any]] = {}

    async def connect(self):
        import websockets
        self.ws = await websockets.connect(self.url, ping_interval=20, ping_timeout=20)
        first = json.loads(await self.ws.recv())
        if first.get("type") == "auth_required":
            await self.ws.send(json.dumps({"type": "auth", "access_token": self.token}))
            ok = json.loads(await self.ws.recv())
            if ok.get("type") != "auth_ok":
                raise RuntimeError(f"WS auth failed: {ok}")
        LOG.info("HA WebSocket connected")

    async def call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.ws:
            raise RuntimeError("WS not connected")
        payload = dict(payload)
        payload.setdefault("id", self._id)
        self._id += 1
        await self.ws.send(json.dumps(payload))
        while True:
            msg = json.loads(await self.ws.recv())
            if msg.get("id") == payload["id"]:
                return msg

    async def prime_states(self):
        res = await self.call({"type": "get_states"})
        states = res.get("result", res.get("data", [])) or []
        for s in states:
            self.state_cache[s["entity_id"]] = s

    async def subscribe_state_changes(self, on_event):
        sub_id = self._id
        self._id += 1
        await self.ws.send(json.dumps(
            {"id": sub_id, "type": "subscribe_events", "event_type": "state_changed"}
        ))

        async def _loop():
            while True:
                raw = await self.ws.recv()
                evt = json.loads(raw)
                if evt.get("type") == "event" and evt.get("event", {}).get("event_type") == "state_changed":
                    data = evt["event"]["data"]
                    ent_id = data["entity_id"]
                    self.state_cache[ent_id] = data.get("new_state") or {}
                    await on_event(data)   # wir geben nur den 'data'-Teil weiter

        asyncio.create_task(_loop())

    def get_value(self, entity_id: str, mode="state", attr: Optional[str] = None, analog=False):
        st = self.state_cache.get(entity_id)
        if not st:
            return None
        val = st.get("state") if mode == "state" else (st.get("attributes") or {}).get(attr)
        if analog:
            try:
                return float(val)
            except Exception:
                return 0.0
        return str(val).lower() in ("on","true","1","open","heat","cool")

    async def call_service(self, domain: str, service: str, data: Dict[str, Any]):
        res = await self.call({"type": "call_service", "domain": domain, "service": service, "service_data": data})
        if not res.get("success", True):
            LOG.warning("Service call failed: %s", res)

# -----------------------------------------------------------
# Mapping / Objektdefinition
# -----------------------------------------------------------
@dataclass
class Mapping:
    entity_id: str
    object_type: str        # analogValue | binaryValue
    instance: int
    units: Optional[str] = None
    writable: bool = False
    mode: str = "state"     # state | attr
    attr: Optional[str] = None
    name: Optional[str] = None
    write: Optional[Dict[str, Any]] = None

ENGINEERING_UNITS_ENUM = {"degreesCelsius": 62, "percent": 98, "noUnits": 95}
SUPPORTED_TYPES = {"analogValue","binaryValue"}

# -----------------------------------------------------------
# BACpypes Imports (lazy)
# -----------------------------------------------------------
def import_bacpypes():
    from importlib import import_module
    _Application = import_module("bacpypes3.app").Application
    _SimpleArgumentParser = import_module("bacpypes3.argparse").SimpleArgumentParser
    _DeviceObject = import_module("bacpypes3.local.device").DeviceObject
    _Unsigned = import_module("bacpypes3.primitivedata").Unsigned
    _Address = import_module("bacpypes3.pdu").Address

    AV = BV = None
    for mod, av, bv in [
        ("bacpypes3.local.object", "AnalogValueObject", "BinaryValueObject"),
        ("bacpypes3.local.objects.value", "AnalogValueObject", None),
        ("bacpypes3.local.objects.binary", None, "BinaryValueObject"),
        ("bacpypes3.local.analog", "AnalogValueObject", None),
        ("bacpypes3.local.binary", None, "BinaryValueObject"),
    ]:
        try:
            m = import_module(mod)
            if av and hasattr(m, av): AV = getattr(m, av)
            if bv and hasattr(m, bv): BV = getattr(m, bv)
        except Exception:
            pass
    if not AV or not BV:
        raise ImportError("AnalogValueObject/BinaryValueObject not found")

    return _Application, _SimpleArgumentParser, _DeviceObject, AV, BV, _Unsigned, _Address

# -----------------------------------------------------------
# Server
# -----------------------------------------------------------
class Server:
    def __init__(self, cfg_path: str = DEFAULT_CONFIG_PATH):
        self.cfg_path = cfg_path
        self.cfg: Dict[str, Any] = {}
        self.mappings: List[Mapping] = []

        self.ha: Optional[HAWS] = None
        self.app = None
        self.device = None

        # NEU: entity_id -> bacpypes Objekt (für schnelle Updates)
        self.entity_index: Dict[str, Any] = {}

    def load_config(self) -> Dict[str, Any]:
        cfg = _load_yaml_config(self.cfg_path)
        dev = cfg.get("device", {}) or {}
        objs = cfg.get("objects", []) or []
        self.cfg = {
            "device_id": int(dev.get("device_id", 500000)),
            "name": dev.get("name") or "BACnet Hub",
        }
        self.mappings = [Mapping(**o) for o in objs if isinstance(o, dict)]
        return cfg

    async def _add_object(self, app, m: Mapping, AV, BV):
        key = (m.object_type, m.instance)
        name = m.name or m.entity_id

        if m.object_type == "analogValue":
            obj = AV(objectIdentifier=key, objectName=name, presentValue=0.0)
            if m.units and hasattr(obj, "units"):
                units = ENGINEERING_UNITS_ENUM.get(m.units)
                if units is not None:
                    obj.units = units  # type: ignore

            if hasattr(obj, "ReadProperty"):
                orig = obj.ReadProperty
                async def dyn_read(prop, arrayIndex=None):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue" and self.ha:
                        val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
                        try:
                            obj.presentValue = float(val or 0.0)  # type: ignore
                        except Exception:
                            obj.presentValue = 0.0  # type: ignore
                    return await maybe_await(orig(prop, arrayIndex))
                obj.ReadProperty = dyn_read  # type: ignore

            if hasattr(obj, "WriteProperty") and m.writable:
                origw = obj.WriteProperty
                async def dyn_write(prop, value, arrayIndex=None, priority=None, direct=False):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue" and self.ha:
                        await self._write_to_ha(m, value)
                    return await maybe_await(origw(prop, value, arrayIndex, priority, direct))
                obj.WriteProperty = dyn_write  # type: ignore

        else:  # binaryValue
            obj = BV(objectIdentifier=key, objectName=name, presentValue=False)
            if hasattr(obj, "ReadProperty"):
                orig = obj.ReadProperty
                async def dyn_read(prop, arrayIndex=None):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue" and self.ha:
                        val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                        obj.presentValue = bool(val)  # type: ignore
                    return await maybe_await(orig(prop, arrayIndex))
                obj.ReadProperty = dyn_read  # type: ignore

            if hasattr(obj, "WriteProperty") and m.writable:
                origw = obj.WriteProperty
                async def dyn_write(prop, value, arrayIndex=None, priority=None, direct=False):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue" and self.ha:
                        await self._write_to_ha(m, value)
                    return await maybe_await(origw(prop, value, arrayIndex, priority, direct))
                obj.WriteProperty = dyn_write  # type: ignore

        await maybe_await(app.add_object(obj))
        self.entity_index[m.entity_id] = obj  # <— für Live-Updates merken
        LOG.info("Added %s:%s -> %s", *key, m.entity_id)

    async def _write_to_ha(self, m: Mapping, value):
        svc = (m.write or {}).get("service")
        if not (m.writable and svc):
            return
        if svc.endswith(".turn_on_off"):
            domain = svc.split(".",1)[0]
            name = "turn_on" if str(value).lower() in ("1","true","on","active") else "turn_off"
            await self.ha.call_service(domain, name, {"entity_id": m.entity_id})
            return
        if "." in svc:
            domain, service = svc.split(".",1)
            data = {"entity_id": m.entity_id}
            payload_key = (m.write or {}).get("payload_key")
            if payload_key:
                data[payload_key] = value
            await self.ha.call_service(domain, service, data)

    async def start(self):
        # 1) YAML laden → argv für BACpypes bauen
        cfg = self.load_config()
        argv = _build_argv_from_yaml(cfg)

        # 2) HA verbinden
        self.ha = HAWS()
        await self.ha.connect()
        await self.ha.prime_states()
        await self.ha.subscribe_state_changes(self._on_state_changed)

        # 3) BACpypes importieren
        Application, SimpleArgumentParser, DeviceObject, AV, BV, Unsigned, Address = import_bacpypes()

        # 4) Parser + Application.from_args (wie in deinem funktionierenden Beispiel)
        parser = SimpleArgumentParser()
        args = parser.parse_args(argv)  # nur YAML-argv

        app = Application.from_args(args)
        self.app = app

        # Device referenzieren
        dev = getattr(app, "local_device", None)
        if not dev:
            for obj in app.objectIdentifier.values():
                if isinstance(obj, DeviceObject):
                    dev = obj
                    break
        self.device = dev

        # Logs wie erwartet
        if _debug:
            try:
                from bacpypes3.settings import settings as bp_settings
                LOG.debug("args: %r", vars(args))
                LOG.debug("settings: %s", dict(bp_settings))
            except Exception:
                LOG.debug("args: %r", vars(args))

        bind_addr = getattr(args, "address", None) or "0.0.0.0"
        bind_port = getattr(args, "port", None) or 47808
        LOG.info("BACnet bound to %s:%s device-id=%s", bind_addr, bind_port, self.cfg["device_id"])
        LOG.debug("ipv4_address: %r", Address(f"{bind_addr}"))
        if self.device:
            LOG.debug("local_device: %r", self.device); dump_obj_debug("local_device contents:", self.device)
        LOG.debug("app: %r", self.app)

        # 5) Objekte anlegen
        for m in self.mappings:
            if m.object_type not in SUPPORTED_TYPES:
                LOG.warning("Unsupported type %s", m.object_type); continue
            await self._add_object(self.app, m, AV, BV)

    async def _on_state_changed(self, data: Dict[str, Any]):
        """Live-Update der presentValue bei HA-Änderungen."""
        try:
            ent_id = data.get("entity_id")
            if not ent_id:
                return
            obj = self.entity_index.get(ent_id)
            if not obj:
                return

            # passendes Mapping finden
            m = next((mm for mm in self.mappings if mm.entity_id == ent_id), None)
            if not m:
                return

            new_state = data.get("new_state") or {}
            # Wert extrahieren
            if m.mode == "attr" and m.attr:
                val = (new_state.get("attributes") or {}).get(m.attr)
            else:
                val = new_state.get("state")

            # in BACnet-Objekt schreiben
            if m.object_type == "analogValue":
                try:
                    if val in (None, "", "unknown", "unavailable"):
                        obj.presentValue = 0.0  # type: ignore
                    else:
                        obj.presentValue = float(val)  # type: ignore
                except Exception:
                    obj.presentValue = 0.0  # type: ignore
            else:  # binaryValue
                obj.presentValue = str(val).lower() in ("on","true","1","open","heat","cool")  # type: ignore

            LOG.debug("HA change -> %s:%s presentValue=%r", m.object_type, m.instance, obj.presentValue)
            # Hinweis: COV-Notifications könnten hier optional gesendet werden.
        except Exception as exc:
            LOG.debug("state_changed handling failed: %s", exc)

    async def run_forever(self):
        await self.start()
        stopper = asyncio.Event()
        try:
            await stopper.wait()
        finally:
            if self.app:
                close = getattr(self.app, "close", None)
                if callable(close):
                    res = close()
                    if inspect.isawaitable(res):
                        await res

# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(Server().run_forever())
    except KeyboardInterrupt:
        pass
