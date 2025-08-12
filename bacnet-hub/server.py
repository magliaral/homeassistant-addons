from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import os
import tempfile
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
LOG_APDU = bool(OPTS.get("log_apdu", False))

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

def _pid_is_present_value(prop) -> bool:
    """Erkenne 'present-value' robust (Enum/String/camel/underscore/hyphen)."""
    try:
        pid = getattr(prop, "propertyIdentifier", prop)
    except Exception:
        pid = prop
    text = None
    for attr in ("name", "value"):
        if hasattr(pid, attr):
            v = getattr(pid, attr)
            if isinstance(v, str):
                text = v
                break
    if text is None:
        text = str(pid)
    norm = text.replace("-", "").replace("_", "").lower()
    return norm == "presentvalue"

# -----------------------------------------------------------
# YAML laden und argv/Parser für BACpypes bauen
# -----------------------------------------------------------
DEFAULT_CFG_DIR = "/config/bacnet-hub"
DEFAULT_CONFIG_PATH = f"{DEFAULT_CFG_DIR}/mappings.yaml"
DEFAULT_BACPY_YAML_PATH = f"{DEFAULT_CFG_DIR}/bacpypes.yml"

def _load_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as err:
        LOG.warning("Fehler beim Laden von YAML %s: %r", path, err)
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

    dev = config.get("device", {}) or {}
    if "--device-instance" not in argv and "-i" not in argv and dev.get("device_id"):
        argv.extend(["--device-instance", str(int(dev["device_id"]))])
    if dev.get("address"):
        argv.extend(["--address", str(dev["address"])])
    if dev.get("port"):
        argv.extend(["--port", str(int(dev.get("port")))])

    if _debug:
        LOG.debug("Finale argv aus YAML: %r", argv)
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
                    await on_event(data)

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
    _YAMLArgumentParser = import_module("bacpypes3.argparse").YAMLArgumentParser
    _DeviceObject = import_module("bacpypes3.local.device").DeviceObject
    _Unsigned = import_module("bacpypes3.primitivedata").Unsigned
    _Address = import_module("bacpypes3.pdu").Address

    # Typen zum Entpacken:
    _prim = import_module("bacpypes3.primitivedata")
    AnyPD = getattr(_prim, "Any")
    RealPD = getattr(_prim, "Real")
    BooleanPD = getattr(_prim, "Boolean")

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

    # Optional: BinaryPV Enum für active/inactive
    BinaryPV = None
    try:
        BinaryPV = import_module("bacpypes3.basetypes").BinaryPV
    except Exception:
        BinaryPV = None

    # APDU Klassen für Logging
    _apdu = import_module("bacpypes3.apdu")
    WritePropertyRequest = getattr(_apdu, "WritePropertyRequest", None)
    WritePropertyMultipleRequest = getattr(_apdu, "WritePropertyMultipleRequest", None)

    return (_Application, _SimpleArgumentParser, _YAMLArgumentParser, _DeviceObject,
            AV, BV, _Unsigned, _Address, BinaryPV, WritePropertyRequest, WritePropertyMultipleRequest,
            AnyPD, RealPD, BooleanPD)

# -----------------------------------------------------------
# Custom Local Objects (Overrides)
# -----------------------------------------------------------
class CustomAnalogValueObject:
    def __init__(self, base_cls, mapping: Mapping, server: "Server", AnyPD, RealPD):
        class _AV(base_cls):  # type: ignore[misc]
            async def ReadProperty(self, prop, arrayIndex=None):  # type: ignore[override]
                if _pid_is_present_value(prop) and server.ha:
                    val = server.ha.get_value(mapping.entity_id, mapping.mode, mapping.attr, analog=True)
                    try:
                        self.presentValue = float(val or 0.0)  # type: ignore[attr-defined]
                    except Exception:
                        self.presentValue = 0.0  # type: ignore[attr-defined]
                return await super().ReadProperty(prop, arrayIndex)  # type: ignore[misc]

            async def WriteProperty(self, prop, value, arrayIndex=None, priority=None, direct=False):  # type: ignore[override]
                if _pid_is_present_value(prop) and server.ha and mapping.writable:
                    raw = value
                    # --- Any → Real entpacken ---
                    try:
                        if isinstance(raw, AnyPD):
                            try:
                                raw = raw.cast(RealPD).get_value()
                            except Exception:
                                raw = raw.get_value()
                        if hasattr(raw, "get_value"):
                            raw = raw.get_value()
                        elif hasattr(raw, "value"):
                            raw = raw.value
                        num = float(raw)
                    except Exception:
                        LOG.info("AV Write %s:%s -> unparsable %r (fallback 0.0)",
                                 mapping.object_type, mapping.instance, value)
                        num = 0.0
                    LOG.info("AV Write %s:%s -> %s (prio=%s) -> HA",
                             mapping.object_type, mapping.instance, num, priority)
                    await server._write_to_ha(mapping, num)
                return await super().WriteProperty(prop, value, arrayIndex, priority, direct)  # type: ignore[misc]
        self.cls = _AV

class CustomBinaryValueObject:
    def __init__(self, base_cls, mapping: Mapping, server: "Server", BinaryPV, AnyPD, BooleanPD):
        class _BV(base_cls):  # type: ignore[misc]
            async def ReadProperty(self, prop, arrayIndex=None):  # type: ignore[override]
                if _pid_is_present_value(prop) and server.ha:
                    val = server.ha.get_value(mapping.entity_id, mapping.mode, mapping.attr, analog=False)
                    self.presentValue = bool(val)  # type: ignore[attr-defined]
                return await super().ReadProperty(prop, arrayIndex)  # type: ignore[misc]

            async def WriteProperty(self, prop, value, arrayIndex=None, priority=None, direct=False):  # type: ignore[override]
                if _pid_is_present_value(prop) and server.ha and mapping.writable:
                    raw = value
                    # --- Any → BinaryPV / Boolean entpacken ---
                    try:
                        if isinstance(raw, AnyPD):
                            # Erst versuchen BinaryPV (active/inactive), sonst Boolean
                            if BinaryPV is not None:
                                try:
                                    raw = raw.cast(BinaryPV).get_value()
                                except Exception:
                                    pass
                            if hasattr(raw, "get_value"):
                                raw = raw.get_value()
                            # Falls immer noch Any → Boolean probieren:
                            if isinstance(raw, AnyPD):
                                try:
                                    raw = raw.cast(BooleanPD).get_value()
                                except Exception:
                                    raw = str(raw)
                        elif hasattr(raw, "get_value"):
                            raw = raw.get_value()
                        elif hasattr(raw, "value"):
                            raw = raw.value
                    except Exception:
                        pass

                    on = False
                    s = str(raw).lower()
                    if s in ("1","true","on","active","open"):
                        on = True
                    elif s in ("0","false","off","inactive","closed"):
                        on = False
                    elif "active" in s:
                        on = True
                    # Enum direkt vergleichen:
                    if not on and BinaryPV is not None:
                        try:
                            if raw == getattr(BinaryPV, "active"):
                                on = True
                            elif raw == getattr(BinaryPV, "inactive"):
                                on = False
                        except Exception:
                            pass

                    LOG.info("BV Write %s:%s -> %s (prio=%s) -> HA",
                             mapping.object_type, mapping.instance, on, priority)
                    await server._write_to_ha(mapping, on)
                return await super().WriteProperty(prop, value, arrayIndex, priority, direct)  # type: ignore[misc]
        self.cls = _BV

# -----------------------------------------------------------
# Custom Application mit APDU-Logging (WriteProperty)
# -----------------------------------------------------------
def make_custom_application(base_cls, WritePropertyRequest, WritePropertyMultipleRequest):
    @bacpypes_debugging
    class CustomApplication(base_cls):  # type: ignore[misc]
        async def do_WritePropertyRequest(self, apdu):  # type: ignore[override]
            try:
                obj = getattr(apdu, "objectIdentifier", None)
                prop = getattr(apdu, "propertyIdentifier", None)
                value = getattr(apdu, "propertyValue", None)
                prio = getattr(apdu, "priority", None)
                LOG.info("APDU WritePropertyRequest obj=%r prop=%r prio=%r value=%r", obj, prop, prio, value)
                if LOG_APDU and hasattr(apdu, "debug_contents"):
                    apdu.debug_contents()
            except Exception:
                LOG.info("APDU WritePropertyRequest (unable to format)")
            return await super().do_WritePropertyRequest(apdu)  # type: ignore[misc]

        async def do_WritePropertyMultipleRequest(self, apdu):  # type: ignore[override]
            try:
                LOG.info("APDU WritePropertyMultipleRequest recv")
                if LOG_APDU and hasattr(apdu, "debug_contents"):
                    apdu.debug_contents()
            except Exception:
                LOG.info("APDU WritePropertyMultipleRequest (unable to format)")
            return await super().do_WritePropertyMultipleRequest(apdu)  # type: ignore[misc]
    return CustomApplication

# -----------------------------------------------------------
# Server
# -----------------------------------------------------------
class Server:
    def __init__(self, cfg_path: str = DEFAULT_CONFIG_PATH):
        self.cfg_path = cfg_path
        self.cfg_all: Dict[str, Any] = {}
        self.cfg_device: Dict[str, Any] = {}
        self.mappings: List[Mapping] = []

        self.ha: Optional[HAWS] = None
        self.app = None
        self.device = None

        # entity_id -> bacpypes Objekt
        self.entity_index: Dict[str, Any] = {}

    def load_config(self) -> None:
        cfg = _load_yaml(self.cfg_path)
        self.cfg_all = cfg
        dev = cfg.get("device", {}) or {}
        objs = cfg.get("objects", []) or []
        self.cfg_device = {
            "device_id": int(dev.get("device_id", 500000)),
            "name": dev.get("name") or "BACnet Hub",
        }
        self.mappings = [Mapping(**o) for o in objs if isinstance(o, dict)]

    def build_bacpypes_args(self, YAMLArgumentParser, SimpleArgumentParser):
        # 1) eigene bacpypes.yml?
        if os.path.exists(DEFAULT_BACPY_YAML_PATH):
            parser = YAMLArgumentParser()
            return parser.parse_args(["--yaml", DEFAULT_BACPY_YAML_PATH])

        # 2) eingebetteter Block in mappings.yaml?
        bp_yaml = self.cfg_all.get("bacpypes_yaml")
        if isinstance(bp_yaml, dict):
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yml") as tf:
                yaml.safe_dump(bp_yaml, tf)
                temp_path = tf.name
            parser = YAMLArgumentParser()
            args = parser.parse_args(["--yaml", temp_path])
            try:
                os.remove(temp_path)
            except OSError:
                pass
            return args

        # 3) Fallback: argv bauen
        argv = _build_argv_from_yaml(self.cfg_all)
        parser = SimpleArgumentParser()
        return parser.parse_args(argv)

    async def _add_object(self, app, m: Mapping, AV, BV, BinaryPV, AnyPD, RealPD, BooleanPD):
        key = (m.object_type, m.instance)
        name = m.name or m.entity_id

        if m.object_type == "analogValue":
            AVCls = CustomAnalogValueObject(AV, m, self, AnyPD, RealPD).cls
            obj = AVCls(objectIdentifier=key, objectName=name, presentValue=0.0)
            if m.units and hasattr(obj, "units"):
                units = ENGINEERING_UNITS_ENUM.get(m.units)
                if units is not None:
                    obj.units = units  # type: ignore[attr-defined]
        else:
            BVCls = CustomBinaryValueObject(BV, m, self, BinaryPV, AnyPD, BooleanPD).cls
            obj = BVCls(objectIdentifier=key, objectName=name, presentValue=False)

        await maybe_await(app.add_object(obj))
        self.entity_index[m.entity_id] = obj
        LOG.info("Added %s:%s -> %s", *key, m.entity_id)

    async def _write_to_ha(self, m: Mapping, value):
        """
        Führt den in m.write.service angegebenen HA-Service aus.
        Unterstützt:
          - "<domain>.turn_on_off"  -> automatisch on/off
          - "<domain>.<service>"    -> generisch + optional payload_key / extra
        """
        if not (m.writable and m.write and m.write.get("service")):
            LOG.warning("Write ignored (kein Service definiert) für %s", m.entity_id)
            return

        svc = m.write["service"]

        if svc.endswith(".turn_on_off"):
            domain = svc.split(".", 1)[0]
            name = "turn_on" if bool(value) else "turn_off"
            data = {"entity_id": m.entity_id}
            LOG.info("Call service %s.%s data=%s", domain, name, data)
            await self.ha.call_service(domain, name, data)
            return

        if "." in svc:
            domain, service = svc.split(".", 1)
            data = {"entity_id": m.entity_id}
            payload_key = m.write.get("payload_key")
            if payload_key is not None:
                data[payload_key] = value
            extra = m.write.get("extra")
            if isinstance(extra, dict):
                data.update(extra)
            LOG.info("Call service %s.%s data=%s", domain, service, data)
            await self.ha.call_service(domain, service, data)
            return

        LOG.warning("Unbekanntes Serviceformat: %s", svc)

    async def _initial_sync(self):
        """Alle aktuellen HA-Werte sofort in die BACnet-Objekte schreiben."""
        for m in self.mappings:
            obj = self.entity_index.get(m.entity_id)
            if not obj:
                continue
            if m.object_type == "analogValue":
                val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
                try:
                    obj.presentValue = float(val or 0.0)  # type: ignore[attr-defined]
                except Exception:
                    obj.presentValue = 0.0  # type: ignore[attr-defined]
                LOG.debug("Initial sync AV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)
            else:
                val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                obj.presentValue = bool(val)  # type: ignore[attr-defined]
                LOG.debug("Initial sync BV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)

    async def start(self):
        # 1) YAML laden
        self.load_config()

        # 2) HA verbinden
        self.ha = HAWS()
        await self.ha.connect()
        await self.ha.prime_states()

        # 3) BACpypes importieren & Parser/Args vorbereiten
        (BaseApplication, SimpleArgumentParser, YAMLArgumentParser, DeviceObject,
         AV, BV, Unsigned, Address, BinaryPV, WPR, WPMR,
         AnyPD, RealPD, BooleanPD) = import_bacpypes()

        # 4) Custom Application-Klasse mit Write-APDU-Logging
        CustomApplication = make_custom_application(BaseApplication, WPR, WPMR)

        args = self.build_bacpypes_args(YAMLArgumentParser, SimpleArgumentParser)

        # 5) Application starten
        app = CustomApplication.from_args(args)   # <— unsere Subklasse!
        self.app = app

        # Device referenzieren
        dev = getattr(app, "local_device", None)
        if not dev:
            for obj in app.objectIdentifier.values():
                if isinstance(obj, DeviceObject):
                    dev = obj
                    break
        self.device = dev

        # Logs
        if _debug:
            try:
                from bacpypes3.settings import settings as bp_settings
                LOG.debug("args: %r", vars(args))
                LOG.debug("settings: %s", dict(bp_settings))
            except Exception:
                LOG.debug("args: %r", vars(args))

        bind_addr = getattr(args, "address", None) or "0.0.0.0"
        bind_port = getattr(args, "port", None) or 47808
        LOG.info("BACnet bound to %s:%s device-id=%s", bind_addr, bind_port, self.cfg_device.get("device_id"))
        LOG.debug("ipv4_address: %r", Address(f"{bind_addr}"))
        if self.device:
            LOG.debug("local_device: %r", self.device); dump_obj_debug("local_device contents:", self.device)
        LOG.debug("app: %r", self.app)

        # 6) Objekte anlegen
        for m in self.mappings:
            if m.object_type not in SUPPORTED_TYPES:
                LOG.warning("Unsupported type %s", m.object_type); continue
            await self._add_object(self.app, m, AV, BV, BinaryPV, AnyPD, RealPD, BooleanPD)

        # 7) Initiale Synchronisierung
        await self._initial_sync()

        # 8) Live-Events abonnieren
        await self.ha.subscribe_state_changes(self._on_state_changed)

    async def _on_state_changed(self, data: Dict[str, Any]):
        """Live-Update der presentValue bei HA-Änderungen."""
        try:
            ent_id = data.get("entity_id")
            if not ent_id:
                return
            obj = self.entity_index.get(ent_id)
            if not obj:
                return
            m = next((mm for mm in self.mappings if mm.entity_id == ent_id), None)
            if not m:
                return
            new_state = data.get("new_state") or {}
            if m.mode == "attr" and m.attr:
                val = (new_state.get("attributes") or {}).get(m.attr)
            else:
                val = new_state.get("state")
            if m.object_type == "analogValue":
                try:
                    if val in (None, "", "unknown", "unavailable"):
                        obj.presentValue = 0.0  # type: ignore[attr-defined]
                    else:
                        obj.presentValue = float(val)  # type: ignore[attr-defined]
                except Exception:
                    obj.presentValue = 0.0  # type: ignore[attr-defined]
            else:
                obj.presentValue = str(val).lower() in ("on","true","1","open","heat","cool")  # type: ignore[attr-defined]
            LOG.debug("HA change -> %s:%s presentValue=%r", m.object_type, m.instance, obj.presentValue)
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
