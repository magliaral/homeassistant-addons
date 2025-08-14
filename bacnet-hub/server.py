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
import types
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------
# Logging (optimiert)
# -----------------------------------------------------------

def _str_to_level(name: str, default: int = logging.INFO) -> int:
    try:
        lvl = getattr(logging, str(name or "").upper())
        if isinstance(lvl, int):
            return lvl
    except Exception:
        pass
    return default


def init_logging(default_level: str = "INFO") -> None:
    """Initialize root logging once, keep it idempotent, tame noisy libs, pipe warnings."""
    if not logging.getLogger().handlers:  # avoid duplicate handlers when imported
        logging.basicConfig(
            level=_str_to_level(os.getenv("LOG_LEVEL", default_level)),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logging.Formatter.converter = time.localtime

    # Quiet down chatty libraries by default; raise if needed later
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.warning)

    # Route Python warnings to logging
    logging.captureWarnings(True)


init_logging(default_level="INFO")
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

# Optionales globales Root-Log-Level aus den Add-on-Optionen
_root_level_name = (OPTS.get("log_level") or os.getenv("LOG_LEVEL") or "INFO")
logging.getLogger().setLevel(_str_to_level(_root_level_name, logging.INFO))

HA_URL = os.getenv("HA_WS_URL", OPTS.get("ha_url") or "ws://supervisor/core/websocket")
LLAT = (OPTS.get("long_lived_token") or "").strip() or None
BACPYPES_LOG_LEVEL = (OPTS.get("bacpypes_log_level") or "info").lower()


# -----------------------------------------------------------
# bacpypes3 Debugging (ModuleLogger + Decorator)
# -----------------------------------------------------------
try:
    from bacpypes3.debugging import bacpypes_debugging, ModuleLogger  # type: ignore
except Exception:  # Fallbacks, wenn bacpypes3.debugging nicht verfügbar ist
    def bacpypes_debugging(cls):  # no-op
        return cls

    class ModuleLogger:  # type: ignore
        def __init__(self, _):
            pass

        def __getattr__(self, _):
            return lambda *a, **k: None


_debug = 0
_log = ModuleLogger(globals())


def configure_bacpypes_debug(level_name: str) -> None:
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    level = level_map.get((level_name or "info").lower(), logging.INFO)

    # Nur bacpypes-spezifische Logger explizit setzen
    logging.getLogger("bacpypes3").setLevel(level)

    # Eigene Hauptlogger optional angleichen (kein Root-Spam)
    for name in ("__main__", "bacnet_hub_addon", "bacpypes3.app"):
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

    # device fallbacks
    dev = config.get("device", {}) or {}
    if dev.get("instance"):
        argv.extend(["--instance", str(int(dev["instance"]))])
    if dev.get("address"):
        argv.extend(["--address", str(dev["address"])])
    if dev.get("port"):
        argv.extend(["--port", str(int(dev["port"]))])

    if _debug:
        LOG.debug("Finale argv aus YAML: %r", argv)
    return argv


# -----------------------------------------------------------
# Home Assistant WebSocket Client (Single-Reader + Pending-Futures)
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
        self._pending: Dict[int, asyncio.Future] = {}
        self._reader_task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._on_event = None  # set by subscribe_state_changes

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

        # Start single reader
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self):
        """Single consumer of ws.recv(); routes replies and events."""
        assert self.ws is not None
        try:
            while True:
                raw = await self.ws.recv()
                msg = json.loads(raw)

                # Response to a call()?
                msg_id = msg.get("id")
                if msg_id is not None and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        fut.set_result(msg)
                    continue

                # Events (state_changed)
                if msg.get("type") == "event":
                    evt = msg.get("event") or {}
                    if evt.get("event_type") == "state_changed":
                        data = evt.get("data") or {}
                        ent_id = data.get("entity_id")
                        if ent_id:
                            self.state_cache[ent_id] = data.get("new_state") or {}
                        if self._on_event:
                            # do not block the reader
                            asyncio.create_task(self._on_event(data))
                    continue

        except asyncio.CancelledError:
            pass
        except Exception as e:
            LOG.warning("HA WS reader stopped: %r", e)

    async def call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send request and await its response via the single reader."""
        if not self.ws:
            raise RuntimeError("WS not connected")

        # allocate id & future
        req = dict(payload)
        req.setdefault("id", self._id)
        self._id += 1
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req["id"]] = fut

        # send serialized
        async with self._send_lock:
            await self.ws.send(json.dumps(req))

        # await response from _reader
        msg = await fut
        return msg

    async def prime_states(self):
        res = await self.call({"type": "get_states"})
        states = res.get("result", res.get("data", [])) or []
        for s in states:
            self.state_cache[s["entity_id"]] = s

    async def subscribe_state_changes(self, on_event):
        """Register callback and send subscribe request (no extra recv loop!)."""
        self._on_event = on_event
        await self.call({"type": "subscribe_events", "event_type": "state_changed"})

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
        return str(val).lower() in ("on", "true", "1", "open", "heat", "cool")

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
    object_type: str  # analogValue | binaryValue
    instance: int
    units: Optional[str] = None
    writable: bool = False
    mode: str = "state"  # state | attr
    attr: Optional[str] = None
    name: Optional[str] = None
    write: Optional[Dict[str, Any]] = None


ENGINEERING_UNITS_ENUM = {"degreesCelsius": 62, "percent": 98, "noUnits": 95}
SUPPORTED_TYPES = {"analogValue", "binaryValue"}


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
            if av and hasattr(m, av):
                AV = getattr(m, av)
            if bv and hasattr(m, bv):
                BV = getattr(m, bv)
        except Exception:
            pass
    if not AV or not BV:
        raise ImportError("AnalogValueObject/BinaryValueObject not found")

    return _Application, _SimpleArgumentParser, _YAMLArgumentParser, _DeviceObject, AV, BV, _Unsigned, _Address


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

        # entity_id -> bacpypes Objekt (für schnelle Updates)
        self.entity_index: Dict[str, Any] = {}

    # ----------------------------
    # Utility: Property-ID extrahieren / normalisieren
    # ----------------------------
    @staticmethod
    def _extract_prop_id(prop) -> str:
        # prop kann Enum, BACnet-Klasse (mit propertyIdentifier) oder String sein
        try:
            pid = getattr(prop, "propertyIdentifier", None)
            if pid:
                return str(pid)
        except Exception:
            pass
        try:
            return str(prop)
        except Exception:
            return "presentValue"

    @staticmethod
    def _norm_pid(pid: str) -> str:
        # "present-value" / "present_value" / "PresentValue" -> "presentvalue"
        return "".join(ch for ch in str(pid) if ch.isalnum()).lower()

    def load_config(self) -> None:
        cfg = _load_yaml(self.cfg_path)
        self.cfg_all = cfg
        dev = cfg.get("options", {}) or {}
        objs = cfg.get("objects", []) or []
        self.cfg_device = {
            "instance": int(dev.get("instance", 500000)),
            "name": dev.get("name") or "BACnet Hub",
        }
        self.mappings = [Mapping(**o) for o in objs if isinstance(o, dict)]

    def build_bacpypes_args(self, YAMLArgumentParser, SimpleArgumentParser):
        """
        Präferenz:
          1) /config/bacnet_hub/bacpypes.yml
          2) bacpypes_yaml: {} in mappings.yaml
          3) Fallback: bacpypes.options/args + device-Felder
        """
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

    # ----------------------------
    # HA-Push / Guard-Helfer
    # ----------------------------
    def _coerce_for_ha(self, m: Mapping, raw_value):
        """Rohwert robust in HA-Payload umwandeln (analog/binary)."""
        if m.object_type == "analogValue":
            try:
                if hasattr(raw_value, "get_value"):
                    raw_value = raw_value.get_value()
                elif hasattr(raw_value, "value"):
                    raw_value = raw_value.value
                return float(raw_value)
            except Exception:
                return 0.0
        else:
            s = str(getattr(raw_value, "value", raw_value)).lower()
            if s in ("1", "true", "on", "active", "open", "heat", "cool"):
                return True
            if s in ("0", "false", "off", "inactive", "closed"):
                return False
            return "active" in s or s == "1"

    def _is_inbound_from_ha(self, obj) -> bool:
        # ACHTUNG: object.__setattr__ benutzen, um bacpypes __setattr__ zu umgehen
        try:
            return bool(getattr(obj, "_ha_inbound_guard"))
        except Exception:
            return False

    def _set_inbound_from_ha(self, obj, flag: bool):
        # Umgehen der bacpypes __setattr__:
        object.__setattr__(obj, "_ha_inbound_guard", bool(flag))

    # ----------------------------
    # Wrapper-Fabriken für Read/Write (unterstützt CamelCase + snake_case)
    # ----------------------------
    def _make_write_wrapper(self, m: Mapping, obj, orig_write, watched_properties: set):
        LOG.debug("_make_write_wrapper")
        async def dyn_write(_self, *args, **kwargs):
            LOG.debug("BACnet change dyn_write - %s : %s", args, kwargs)

            # 1) Property-Identifier robust auslesen + normalisieren
            prop = args[0] if args else kwargs.get("prop") or kwargs.get("property") or None
            pid_raw = self._extract_prop_id(prop)
            pid = self._norm_pid(pid_raw)
            LOG.debug("pid_raw=%s pid_norm=%s", pid_raw, pid)

            # 2) Originalen Write ausführen (BACnet-intern korrekt halten)
            result = await maybe_await(orig_write(*args, **kwargs))
            LOG.debug("BACnet change orig_write result - %s", result)

            # 3) Nachher: ggf. zu HA spiegeln
            if self.ha and (pid in watched_properties):
                if (not self._is_inbound_from_ha(obj)) and m.writable and m.write and m.write.get("service"):
                    pv_after = getattr(obj, "presentValue", None)
                    coerced = self._coerce_for_ha(m, pv_after)
                    LOG.info(
                        "BACnet change via %s %s:%s -> PV=%s -> push to HA",
                        pid_raw, m.object_type, m.instance, coerced
                    )
                    asyncio.create_task(self._write_to_ha(m, coerced))
            return result
        return dyn_write

    def _make_read_wrapper(self, m: Mapping, obj, orig_read):
        async def dyn_read(_self, *args, **kwargs):
            prop = args[0] if args else kwargs.get("prop") or kwargs.get("property") or None
            pid_raw = self._extract_prop_id(prop)
            pid = self._norm_pid(pid_raw)

            if pid == "presentvalue" and self.ha:
                # JIT-Refresh aus HA
                if m.object_type == "analogValue":
                    val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
                    try:
                        obj.presentValue = float(val or 0.0)  # type: ignore
                    except Exception:
                        obj.presentValue = 0.0  # type: ignore
                else:
                    val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                    obj.presentValue = bool(val)  # type: ignore

            return await maybe_await(orig_read(*args, **kwargs))
        return dyn_read

    async def _add_object(self, app, m: Mapping, AV, BV):
        key = (m.object_type, m.instance)
        name = m.name or m.entity_id

        # normalisierte watched properties
        watched_properties = {"presentvalue", "priorityarray", "relinquishdefault", "outofservice"}

        if m.object_type == "analogValue":
            obj = AV(objectIdentifier=key, objectName=name, presentValue=0.0)
            if m.units and hasattr(obj, "units"):
                units = ENGINEERING_UNITS_ENUM.get(m.units)
                if units is not None:
                    obj.units = units  # type: ignore
            # COV defensiv initialisieren
            try:
                if hasattr(obj, "covIncrement") and getattr(obj, "covIncrement", None) is None:
                    obj.covIncrement = 0.0  # type: ignore[attr-defined]
            except Exception as e:
                LOG.debug("covIncrement init skipped: %r", e)
        else:  # binaryValue
            obj = BV(objectIdentifier=key, objectName=name, presentValue=False)

        # --- Hooks setzen: ReadProperty / read_property ---
        try:
            orig_read_camel = getattr(obj, "ReadProperty", None)
            if callable(orig_read_camel):
                rp = types.MethodType(self._make_read_wrapper(m, obj, orig_read_camel), obj)
                object.__setattr__(obj, "ReadProperty", rp)
                LOG.debug("Hooked ReadProperty for %s:%s", *key)
        except Exception as e:
            LOG.debug("Failed to hook ReadProperty for %s:%s: %r", *key, e)

        try:
            orig_read_snake = getattr(obj, "read_property", None)
            if callable(orig_read_snake):
                rp_snake = types.MethodType(self._make_read_wrapper(m, obj, orig_read_snake), obj)
                object.__setattr__(obj, "read_property", rp_snake)
                LOG.debug("Hooked read_property for %s:%s", *key)
        except Exception as e:
            LOG.debug("Failed to hook read_property for %s:%s: %r", *key, e)

        # --- Hooks setzen: WriteProperty / write_property ---
        try:
            orig_write_camel = getattr(obj, "WriteProperty", None)
            if callable(orig_write_camel):
                wp = types.MethodType(self._make_write_wrapper(m, obj, orig_write_camel, watched_properties), obj)
                object.__setattr__(obj, "WriteProperty", wp)
                LOG.debug("Hooked WriteProperty for %s:%s", *key)
        except Exception as e:
            LOG.debug("Failed to hook WriteProperty for %s:%s: %r", *key, e)

        try:
            orig_write_snake = getattr(obj, "write_property", None)
            if callable(orig_write_snake):
                wp_snake = types.MethodType(self._make_write_wrapper(m, obj, orig_write_snake, watched_properties), obj)
                object.__setattr__(obj, "write_property", wp_snake)
                LOG.debug("Hooked write_property for %s:%s", *key)
        except Exception as e:
            LOG.debug("Failed to hook write_property for %s:%s: %r", *key, e)

        await maybe_await(app.add_object(obj))
        self.entity_index[m.entity_id] = obj  # für Live-Updates merken
        LOG.info("Added %s:%s -> %s", *key, m.entity_id)

    async def _write_to_ha(self, m: Mapping, value):
        """
        Führt den in m.write.service angegebenen HA-Service aus.
        Unterstützt:
          - "<domain>.turn_on_off"  -> automatisch on/off
          - "<domain>.<service>"    -> generisch + optional payload_key / extra
        """
        if not (m.writable and m.write and m.write.get("service")):
            LOG.debug("Write ignored (kein Service definiert) für %s", m.entity_id)
            return

        svc = m.write["service"]

        # Kurzform: turn_on_off (light/switch etc.)
        if svc.endswith(".turn_on_off"):
            domain = svc.split(".", 1)[0]
            name = "turn_on" if bool(value) else "turn_off"
            data = {"entity_id": m.entity_id}
            LOG.info("Call service %s.%s data=%s", domain, name, data)
            await self.ha.call_service(domain, name, data)
            return

        # generischer Service
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

        LOG.debug("Unbekanntes Serviceformat: %s", svc)

    async def _initial_sync(self):
        """Alle aktuellen HA-Werte sofort in die BACnet-Objekte schreiben (dedupliziert)."""
        for ent_id, obj in self.entity_index.items():
            # passendes Mapping holen
            m = next((mm for mm in self.mappings if mm.entity_id == ent_id), None)
            if not m:
                continue

            if m.object_type == "analogValue":
                val = self.ha.get_value(ent_id, m.mode, m.attr, analog=True)
                try:
                    obj.presentValue = float(val or 0.0)  # type: ignore
                except Exception:
                    obj.presentValue = 0.0  # type: ignore
                LOG.debug("Initial sync AV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)
            else:
                val = self.ha.get_value(ent_id, m.mode, m.attr, analog=False)
                obj.presentValue = bool(val)  # type: ignore
                LOG.debug("Initial sync BV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)

        LOG.debug("Mappings count: %d", len(self.mappings))
        LOG.debug("Entity index count: %d (unique entities)", len(self.entity_index))
        dupes = [e for e in {m.entity_id for m in self.mappings} if sum(1 for mm in self.mappings if mm.entity_id == e) > 1]
        if dupes:
            LOG.debug("Duplicate entity_ids in mappings: %r", dupes)

    async def start(self):
        # 1) YAML laden
        self.load_config()

        # 2) HA verbinden
        self.ha = HAWS()
        await self.ha.connect()
        await self.ha.prime_states()

        # 3) BACpypes importieren und Parser/Args erstellen (YAMLArgumentParser bevorzugt)
        (
            Application,
            SimpleArgumentParser,
            YAMLArgumentParser,
            DeviceObject,
            AV,
            BV,
            Unsigned,
            Address,
        ) = import_bacpypes()
        args = self.build_bacpypes_args(YAMLArgumentParser, SimpleArgumentParser)

        # 4) Application starten
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
        bind_instance = getattr(args, "instance", None) or 47808
        LOG.info("BACnet bound to %s:%s device-id=%s", bind_addr, bind_port, bind_instance)

        # 5) Objekte anlegen
        for m in self.mappings:
            if m.object_type not in SUPPORTED_TYPES:
                LOG.warning("Unsupported type %s", m.object_type)
                continue
            await self._add_object(self.app, m, AV, BV)

        # 6) **Initiale Synchronisierung**
        await self._initial_sync()

        # 7) Live-Events abonnieren (nur Request senden; Reader empfängt)
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

            # --- GUARD setzen: diese Änderung stammt aus HA ---
            self._set_inbound_from_ha(obj, True)
            try:
                # in BACnet-Objekt schreiben (normal setzen, damit Typen passen)
                if m.object_type == "analogValue":
                    try:
                        if val in (None, "", "unknown", "unavailable"):
                            obj.presentValue = 0.0  # type: ignore
                        else:
                            obj.presentValue = float(val)  # type: ignore
                    except Exception:
                        obj.presentValue = 0.0  # type: ignore
                else:  # binaryValue
                    obj.presentValue = str(val).lower() in (
                        "on", "true", "1", "open", "heat", "cool"
                    )  # type: ignore
            finally:
                # --- GUARD wieder entfernen ---
                self._set_inbound_from_ha(obj, False)

            LOG.info("HA change -> %s:%s presentValue=%r", m.object_type, m.instance, obj.presentValue)
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
