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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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

_debug = 0
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
    for name in ("__main__"):
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
            if s in ("1","true","on","active","open","heat","cool"):
                return True
            if s in ("0","false","off","inactive","closed"):
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

    async def _add_object(self, app, m: Mapping, AV, BV):
        key = (m.object_type, m.instance)
        name = m.name or m.entity_id

        # Welche Property-Writes sollen HA-Updates triggern?
        watched_properties = {"presentValue", "priorityArray", "relinquishDefault", "outOfService"}

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

            if hasattr(obj, "WriteProperty"):
                origw = obj.WriteProperty
                async def dyn_write(prop, value, arrayIndex=None, priority=None, direct=False):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    # 1) Originalen Write ausführen (damit PV/Prio-Logik korrekt ist)
                    result = await maybe_await(origw(prop, value, arrayIndex, priority, direct))

                    # 2) Falls vom BACnet kommend, PUSH zu HA (wenn gewünscht)
                    if self.ha and (pid in watched_properties):
                        if not self._is_inbound_from_ha(obj) and m.writable and m.write and m.write.get("service"):
                            pv_after = getattr(obj, "presentValue", None)
                            num = self._coerce_for_ha(m, pv_after)
                            LOG.info("AV Change via %s %s:%s -> PV=%s -> HA", pid, m.object_type, m.instance, num)
                            asyncio.create_task(self._write_to_ha(m, num))

                    return result
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

            if hasattr(obj, "WriteProperty"):
                origw = obj.WriteProperty
                async def dyn_write(prop, value, arrayIndex=None, priority=None, direct=False):
                    pid = getattr(prop, "propertyIdentifier", str(prop))

                    # 1) Originalen Write zuerst
                    result = await maybe_await(origw(prop, value, arrayIndex, priority, direct))

                    # 2) Danach effektiven PV holen und (wenn gewünscht) nach HA pushen
                    if self.ha and (pid in watched_properties):
                        if not self._is_inbound_from_ha(obj) and m.writable and m.write and m.write.get("service"):
                            pv_after = getattr(obj, "presentValue", None)
                            on = self._coerce_for_ha(m, pv_after)
                            LOG.info("BV Change via %s %s:%s -> PV=%s -> HA", pid, m.object_type, m.instance, on)
                            asyncio.create_task(self._write_to_ha(m, on))

                    return result
                obj.WriteProperty = dyn_write  # type: ignore

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
        """Alle aktuellen HA-Werte sofort in die BACnet-Objekte schreiben."""
        for m in self.mappings:
            obj = self.entity_index.get(m.entity_id)
            if not obj:
                continue
            if m.object_type == "analogValue":
                val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
                try:
                    obj.presentValue = float(val or 0.0)  # type: ignore
                except Exception:
                    obj.presentValue = 0.0  # type: ignore
                LOG.debug("Initial sync AV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)
            else:
                val = self.ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                obj.presentValue = bool(val)  # type: ignore
                LOG.debug("Initial sync BV %s:%s -> %r", m.object_type, m.instance, obj.presentValue)

    async def start(self):
        # 1) YAML laden
        self.load_config()

        # 2) HA verbinden
        self.ha = HAWS()
        await self.ha.connect()
        await self.ha.prime_states()

        # 3) BACpypes importieren und Parser/Args erstellen (YAMLArgumentParser bevorzugt)
        Application, SimpleArgumentParser, YAMLArgumentParser, DeviceObject, AV, BV, Unsigned, Address = import_bacpypes()
        args = self.build_bacpypes_args(YAMLArgumentParser, SimpleArgumentParser)

        # 4) Application starten (wie in deinem funktionierenden Beispiel)
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
        LOG.debug("ipv4_address: %r", Address(f"{bind_addr}"))
        if self.device:
            LOG.debug("local_device: %r", self.device); dump_obj_debug("local_device contents:", self.device)
        LOG.debug("app: %r", self.app)

        # 5) Objekte anlegen
        for m in self.mappings:
            if m.object_type not in SUPPORTED_TYPES:
                LOG.warning("Unsupported type %s", m.object_type); continue
            await self._add_object(self.app, m, AV, BV)

        # 6) **Initiale Synchronisierung**
        await self._initial_sync()

        # 7) Live-Events abonnieren
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
            finally:
                # --- GUARD wieder entfernen ---
                self._set_inbound_from_ha(obj, False)

            LOG.info("HA change -> %s:%s presentValue=%r", m.object_type, m.instance, obj.presentValue)
            # (optional) COV-Notifications könnten hier gesendet werden.
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
