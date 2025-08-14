from __future__ import annotations

import asyncio
import contextlib
import inspect
import io
import json
import logging
import os
import tempfile
import time
import yaml
import types
import random
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
    logging.getLogger("asyncio").setLevel(logging.WARNING)

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
    for name in ("__main__", "bacnet_hub_addon", "bacpypes3.app", "bacpypes3.pdu"):
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
# Home Assistant WebSocket Client (robust: retry + reconnect)
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
        self._on_event = None            # set by subscribe_state_changes
        self._reconnect_lock = asyncio.Lock()
        self._connected_evt = asyncio.Event()
        self._want_events = False        # remember subscription intent

    # ---------- public API ----------

    async def connect(self):
        """Connect once with retry/backoff until HA is available."""
        await self._connect_with_retry()

    async def call(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Send request and await its response via the single reader."""
        await self._ensure_connected()

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
        return await fut

    async def prime_states(self):
        res = await self.call({"type": "get_states"})
        states = res.get("result", res.get("data", [])) or []
        for s in states:
            self.state_cache[s["entity_id"]] = s

    async def subscribe_state_changes(self, on_event):
        """Register callback and send subscribe request (no extra recv loop!)."""
        self._on_event = on_event
        self._want_events = True
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

    # ---------- internals ----------

    async def _ensure_connected(self):
        if self.ws is not None and not self.ws.closed and self._connected_evt.is_set():
            return
        await self._connect_with_retry()

    async def _connect_with_retry(self, first_delay: float = 1.0, max_delay: float = 30.0):
        """Loop until connected & authenticated; handles HA not ready yet."""
        async with self._reconnect_lock:
            if self.ws is not None and not self.ws.closed and self._connected_evt.is_set():
                return

            delay = first_delay
            import websockets  # local import to avoid top-level deps problems

            while True:
                try:
                    self._connected_evt.clear()
                    # open TCP + WebSocket handshake
                    self.ws = await websockets.connect(self.url, ping_interval=20, ping_timeout=20)

                    # auth handshake
                    first = json.loads(await self.ws.recv())
                    if first.get("type") == "auth_required":
                        await self.ws.send(json.dumps({"type": "auth", "access_token": self.token}))
                        ok = json.loads(await self.ws.recv())
                        if ok.get("type") != "auth_ok":
                            raise RuntimeError(f"WS auth failed: {ok}")

                    LOG.info("HA WebSocket connected")

                    # start single reader
                    if self._reader_task and not self._reader_task.done():
                        self._reader_task.cancel()
                        with contextlib.suppress(Exception):
                            await self._reader_task
                    self._reader_task = asyncio.create_task(self._reader_loop())

                    # mark connected
                    self._connected_evt.set()

                    # optional re-subscribe + state prime after reconnect
                    if self._want_events and self._on_event:
                        try:
                            await self.subscribe_state_changes(self._on_event)
                        except Exception as e:
                            LOG.warning("Re-subscribe after reconnect failed: %r", e)
                    try:
                        await self.prime_states()
                    except Exception as e:
                        LOG.warning("prime_states after reconnect failed: %r", e)

                    return  # success

                except Exception as e:
                    # HA not ready or network down → backoff & retry
                    LOG.warning(
                        "HA WS connect failed (%s: %s). Retrying in %.1fs ...",
                        e.__class__.__name__, str(e), delay
                    )
                    await asyncio.sleep(delay)
                    delay = min(max_delay, delay * 1.7)

    async def _reader_loop(self):
        """Single consumer of ws.recv(); routes replies and events."""
        import websockets
        assert self.ws is not None
        try:
            while True:
                raw = await self.ws.recv()
                msg = json.loads(raw)

                # route replies
                msg_id = msg.get("id")
                if msg_id is not None and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        fut.set_result(msg)
                    continue

                # route events
                if msg.get("type") == "event":
                    evt = msg.get("event") or {}
                    if evt.get("event_type") == "state_changed":
                        data = evt.get("data") or {}
                        ent_id = data.get("entity_id")
                        if ent_id:
                            self.state_cache[ent_id] = data.get("new_state") or {}
                        if self._on_event:
                            asyncio.create_task(self._on_event(data))
                    continue

        except asyncio.CancelledError:
            pass
        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            LOG.warning("HA WS closed: %s (%s). Will reconnect.", e.__class__.__name__, e)
        except Exception as e:
            LOG.warning("HA WS reader error: %r. Will reconnect.", e)
        finally:
            await self._fail_all_pending(RuntimeError("WebSocket disconnected"))
            self._connected_evt.clear()
            # start reconnect in the background; calls will await _ensure_connected
            asyncio.create_task(self._connect_with_retry())

    async def _fail_all_pending(self, exc: BaseException):
        for _, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()


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
        LOG.debug("Setup write wrapper for %s:%s (%s)", m.object_type, m.instance, m.entity_id)

        async def dyn_write(_self, *args, **kwargs):
            LOG.debug(
                "[WRITE] Incoming BACnet write request → %s:%s (%s) | args=%s kwargs=%s",
                m.object_type, m.instance, m.entity_id, args, kwargs
            )

            # 1) Property-Identifier robust auslesen + normalisieren
            prop = args[0] if args else kwargs.get("prop") or kwargs.get("property") or None
            pid_raw = self._extract_prop_id(prop)
            pid = self._norm_pid(pid_raw)
            LOG.debug("[WRITE] Property ID detected: raw=%s normalized=%s", pid_raw, pid)

            # 2) Originalen Write ausführen (BACnet-intern korrekt halten)
            result = await maybe_await(orig_write(*args, **kwargs))
            LOG.debug("[WRITE] Original BACnet write completed → result=%s", result)

            # 3) Nachher: ggf. zu HA spiegeln
            if self.ha and (pid in watched_properties):
                if self._is_inbound_from_ha(obj):
                    LOG.debug("[WRITE] Change ignored (source=HA)")
                    return result

                if not m.writable:
                    LOG.warning(
                        "[WRITE] BACnet write to %s:%s (%s) ignored → mapping not writable",
                        m.object_type, m.instance, m.entity_id
                    )
                    return result

                if not (m.write and m.write.get("service")):
                    LOG.warning(
                        "[WRITE] BACnet write to %s:%s (%s) ignored → no HA service configured",
                        m.object_type, m.instance, m.entity_id
                    )
                    return result

                # HA-Entity bekannt?
                if not self.ha.state_cache.get(m.entity_id):
                    LOG.warning(
                        "[WRITE] BACnet write to %s:%s (%s) ignored → entity not found in HA cache",
                        m.object_type, m.instance, m.entity_id
                    )
                    return result

                pv_after = getattr(obj, "presentValue", None)
                coerced = self._coerce_for_ha(m, pv_after)
                LOG.info(
                    "[WRITE] BACnet change detected → %s:%s (%s) | Property=%s | PV(after)=%s → Sync to HA",
                    m.object_type, m.instance, m.entity_id, pid_raw, coerced
                )
                asyncio.create_task(self._write_to_ha(m, coerced))
            return result

        return dyn_write

    def _make_read_wrapper(self, m: Mapping, obj, orig_read):
        LOG.debug("Setup read wrapper for %s:%s (%s)", m.object_type, m.instance, m.entity_id)

        async def dyn_read(_self, *args, **kwargs):
            LOG.debug(
                "[READ] BACnet read request → %s:%s (%s) | args=%s kwargs=%s",
                m.object_type, m.instance, m.entity_id, args, kwargs
            )

            # Property-ID ermitteln
            prop = args[0] if args else kwargs.get("prop") or kwargs.get("property") or None
            pid_raw = self._extract_prop_id(prop)
            pid = self._norm_pid(pid_raw)
            LOG.debug("[READ] Property ID detected: raw=%s normalized=%s", pid_raw, pid)

            # Live-Update aus HA vor dem Auslesen
            if pid == "presentvalue" and self.ha:
                if m.object_type == "analogValue":
                    val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
                    try:
                        obj.presentValue = float(val or 0.0)  # type: ignore
                        LOG.debug("[READ] Updated analogValue from HA → %s", obj.presentValue)
                    except Exception as e:
                        LOG.warning("[READ] Could not convert HA value '%s' to float (%s)", val, e)
                        obj.presentValue = 0.0  # type: ignore
                else:
                    val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                    obj.presentValue = bool(val)  # type: ignore
                    LOG.debug("[READ] Updated binaryValue from HA → %s", obj.presentValue)

            result = await maybe_await(orig_read(*args, **kwargs))
            LOG.debug("[READ] Original BACnet read completed → result=%s", result)
            return result

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

        # Entity im Cache vorhanden?
        if not self.ha or not self.ha.state_cache.get(m.entity_id):
            LOG.warning("HA write skipped: entity '%s' nicht im HA-Cache (noch nicht geladen?)", m.entity_id)
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

            # Ist Entity im Cache?
            if not self.ha.state_cache.get(ent_id):
                LOG.warning("Initial sync: Entity '%s' nicht im HA-Cache gefunden", ent_id)

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

    # ----------------------------
    # HA–Verbindung robust aufbauen / überwachen
    # ----------------------------
    async def _connect_ha_with_retry(self):
        """
        Baut die HA-WS-Verbindung mit Exponential Backoff + Jitter auf,
        lädt States und abonniert Events.
        """
        assert self.ha is not None
        attempt = 0
        base = 1.0
        max_sleep = 30.0

        while True:
            attempt += 1
            try:
                await self.ha.connect()
                await self.ha.prime_states()
                await self.ha.subscribe_state_changes(self._on_state_changed)
                LOG.info("HA connection established (attempt %d)", attempt)
                return
            except Exception as e:
                # typischer Fall: HA startet noch oder neu
                sleep_s = min(max_sleep, base * (2 ** (attempt - 1)))
                # Jitter ±20%
                jitter = sleep_s * (0.6 + 0.8 * random.random())
                LOG.warning("HA not ready or WS connect failed (attempt %d): %r → retry in %.1fs",
                            attempt, e, jitter)
                await asyncio.sleep(jitter)

    async def _ha_watchdog(self):
        """Wartet auf Verbindungsabbruch und verbindet dann automatisch neu."""
        assert self.ha is not None
        while True:
            await self.ha.closed_event.wait()
            LOG.warning("HA WebSocket closed → reconnecting…")
            # Neuer Connect + Subscribe + States
            await self._connect_ha_with_retry()
            # Nach Reconnect erneut initial synchronisieren
            try:
                await self._initial_sync()
            except Exception as e:
                LOG.warning("Initial sync after reconnect failed: %r", e)

    # ----------------------------
    # Start / Run
    # ----------------------------
    async def start(self):
        # 1) YAML laden
        self.load_config()

        # 2) HA verbinden (robust)
        self.ha = HAWS()
        await self._connect_ha_with_retry()
        # Watchdog starten
        asyncio.create_task(self._ha_watchdog())

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
