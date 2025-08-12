from __future__ import annotations

import asyncio
import json
import logging
import os
import inspect
import yaml
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# -----------------------------------------------------------
# Grund-Logging (Addon)
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger("bacnet_hub_addon")

# -----------------------------------------------------------
# Add-on-Optionen lesen
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
# bacpypes3 Debug: Decorator + ModuleLogger import (mit Fallback)
# -----------------------------------------------------------
try:
    from bacpypes3.debugging import bacpypes_debugging, ModuleLogger  # type: ignore
except Exception:
    def bacpypes_debugging(cls):  # no-op, falls Paket fehlt
        return cls
    class ModuleLogger:  # minimaler Fallback
        def __init__(self, _): pass
        def __getattr__(self, _): return lambda *a, **k: None

# globales Debug-Flag + Modul-Logger wie im Beispiel
_debug = 1 if BACPYPES_LOG_LEVEL == "debug" else 0
_log = ModuleLogger(globals())

# bacpypes3-Logger auf gewünschtes Level setzen
def configure_bacpypes_debug(level_name: str):
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }
    level = level_map.get((level_name or "info").lower(), logging.INFO)

    logging.getLogger("bacpypes3").setLevel(level)
    for name in (
        "bacpypes3.app",
        "bacpypes3.comm",
        "bacpypes3.pdu",
        "bacpypes3.apdu",
        "bacpypes3.netservice",
        "bacpypes3.service",
        "bacpypes3.local",
    ):
        logging.getLogger(name).setLevel(level)

    LOG.info("bacpypes3 logger level set to '%s'", level_name)

configure_bacpypes_debug(BACPYPES_LOG_LEVEL)

# -----------------------------------------------------------
# kleine Helfer
# -----------------------------------------------------------
async def maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x

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
                "No token found. Enable hassio_api/homeassistant_api in add-on config "
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
        await self.ws.send(
            json.dumps(
                {"id": sub_id, "type": "subscribe_events", "event_type": "state_changed"}
            )
        )

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
        return str(val).lower() in ("on", "true", "1", "open", "heat", "cool")

    async def call_service(self, domain: str, service: str, data: Dict[str, Any]):
        res = await self.call(
            {"type": "call_service", "domain": domain, "service": service, "service_data": data}
        )
        if not res.get("success", True):
            LOG.warning("Service call failed: %s", res)

# -----------------------------------------------------------
# Mapping
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

    @property
    def is_analog(self) -> bool:
        return self.object_type in ("analogValue","analogInput","analogOutput")

# -----------------------------------------------------------
# bacpypes3 lazy-import
# -----------------------------------------------------------
def import_bacpypes():
    from importlib import import_module

    _Application = import_module("bacpypes3.app").Application
    _DeviceObject = import_module("bacpypes3.local.device").DeviceObject
    _Unsigned = import_module("bacpypes3.primitivedata").Unsigned
    _Address = import_module("bacpypes3.pdu").Address
    _IAm = import_module("bacpypes3.apdu").IAmRequest

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

    return _Application, _DeviceObject, AV, BV, _Unsigned, _Address, _IAm

ENGINEERING_UNITS_ENUM = {"degreesCelsius": 62, "percent": 98, "noUnits": 95}
SUPPORTED_TYPES = {"analogValue","binaryValue"}

# -----------------------------------------------------------
# BACnet Server
# -----------------------------------------------------------
class Server:
    def __init__(self, cfg_path="/config/bacnet_hub/mappings.yaml"):
        self.cfg_path = cfg_path
        self.cfg: Dict[str, Any] = {}
        self.mappings: List[Mapping] = []
        self.ha: Optional[HAWS] = None
        self.bp = None
        self.app = None
        self.device = None
        self.objects: Dict[tuple, Mapping] = {}

    def load_config(self):
        if not os.path.exists(self.cfg_path):
            os.makedirs(os.path.dirname(self.cfg_path), exist_ok=True)
            with open(self.cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(
                    {"device": {"device_id": 500000, "name": "BACnet Hub", "address": "0.0.0.0", "port": 47808}, "objects": []},
                    f,
                    sort_keys=False,
                )
        with open(self.cfg_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        dev = raw.get("device", {}) or {}
        objs = raw.get("objects", []) or []
        self.cfg = {
            "device_id": int(dev.get("device_id", 500000)),
            "name": dev.get("name") or "BACnet Hub",
            "address": dev.get("address","0.0.0.0"),
            "port": int(dev.get("port", 47808)),
            "bbmd_ip": dev.get("bbmd_ip"),
            "bbmd_ttl": int(dev.get("bbmd_ttl", 600)) if dev.get("bbmd_ttl") else None,
        }
        self.mappings = [Mapping(**o) for o in objs if isinstance(o, dict)]

    @staticmethod
    async def _add_object(app, m: Mapping, AnalogValueObject, BinaryValueObject, ha: HAWS):
        key = (m.object_type, m.instance)
        name = m.name or m.entity_id

        if m.object_type == "analogValue":
            obj = AnalogValueObject(objectIdentifier=key, objectName=name, presentValue=0.0)
            if m.units and hasattr(obj, "units"):
                units = ENGINEERING_UNITS_ENUM.get(m.units)
                if units is not None:
                    obj.units = units  # type: ignore

            if hasattr(obj, "ReadProperty"):
                orig = obj.ReadProperty
                async def dyn_read(prop, arrayIndex=None):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue":
                        val = ha.get_value(m.entity_id, m.mode, m.attr, analog=True)
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
                    if pid == "presentValue":
                        await Server._write_to_ha(ha, m, value)
                    return await maybe_await(origw(prop, value, arrayIndex, priority, direct))
                obj.WriteProperty = dyn_write  # type: ignore

        else:  # binaryValue
            obj = BinaryValueObject(objectIdentifier=key, objectName=name, presentValue=False)
            if hasattr(obj, "ReadProperty"):
                orig = obj.ReadProperty
                async def dyn_read(prop, arrayIndex=None):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue":
                        val = ha.get_value(m.entity_id, m.mode, m.attr, analog=False)
                        obj.presentValue = bool(val)  # type: ignore
                    return await maybe_await(orig(prop, arrayIndex))
                obj.ReadProperty = dyn_read  # type: ignore

            if hasattr(obj, "WriteProperty") and m.writable:
                origw = obj.WriteProperty
                async def dyn_write(prop, value, arrayIndex=None, priority=None, direct=False):
                    pid = getattr(prop, "propertyIdentifier", str(prop))
                    if pid == "presentValue":
                        await Server._write_to_ha(ha, m, value)
                    return await maybe_await(origw(prop, value, arrayIndex, priority, direct))
                obj.WriteProperty = dyn_write  # type: ignore

        await maybe_await(app.add_object(obj))
        return key

    @staticmethod
    async def _write_to_ha(ha: HAWS, m: Mapping, value):
        if not (m.write and m.writable):
            return
        svc = m.write.get("service")
        if svc and svc.endswith(".turn_on_off"):
            domain = svc.split(".",1)[0]
            name = "turn_on" if str(value).lower() in ("1","true","on","active") else "turn_off"
            await ha.call_service(domain, name, {"entity_id": m.entity_id})
            return
        if svc and "." in svc:
            domain, service = svc.split(".",1)
            data = {"entity_id": m.entity_id}
            payload_key = m.write.get("payload_key")
            if payload_key:
                data[payload_key] = value
            await ha.call_service(domain, service, data)

    async def start(self):
        # 1) Config laden
        self.load_config()

        # 2) HA verbinden
        self.ha = HAWS()
        await self.ha.connect()
        await self.ha.prime_states()
        await self.ha.subscribe_state_changes(self._on_state_changed)

        # 3) bacpypes importieren
        self.bp = await asyncio.get_event_loop().run_in_executor(None, import_bacpypes)
        Application, DeviceObject, AnalogValueObject, BinaryValueObject, Unsigned, Address, IAmRequest = self.bp

        # 4) App-Klasse mit bacpypes_debugging
        @bacpypes_debugging
        class _HAApp(Application):  # type: ignore
            def __init__(_self, device, addr, dev_id):
                if _debug: _log.debug("__init__ device=%r addr=%r dev_id=%r", device, addr, dev_id)
                super().__init__(device, addr)
                _self._dev_id = dev_id

            async def do_WhoIsRequest(_self, apdu):
                if _debug: _log.debug("do_WhoIsRequest %r", apdu)
                try:
                    low = getattr(apdu, "deviceInstanceRangeLowLimit", None)
                    high = getattr(apdu, "deviceInstanceRangeHighLimit", None)
                    if (low is not None and _self._dev_id < low) or (high is not None and _self._dev_id > high):
                        return
                    iam = IAmRequest(
                        iAmDeviceIdentifier=("device", _self._dev_id),
                        maxAPDULengthAccepted=1024,
                        segmentationSupported="noSegmentation",
                        vendorID=999,
                    )
                    try:
                        iam.pduDestination = apdu.pduSource
                    except Exception:
                        iam.pduDestination = Address("255.255.255.255")
                    await _self.response(iam)
                except Exception as exc:
                    LOG.warning("do_WhoIsRequest failed: %s", exc)

        # 5) Device + App
        self.device = DeviceObject(
            objectIdentifier=("device", self.cfg["device_id"]),
            objectName=self.cfg["name"],
            segmentationSupported="noSegmentation",
            vendorIdentifier=Unsigned(999) if Unsigned else 999,
        )
        bind = f'{self.cfg["address"]}:{self.cfg["port"]}'
        self.app = _HAApp(self.device, Address(bind), self.cfg["device_id"])
        LOG.info("BACnet bound to %s device-id=%s", bind, self.cfg["device_id"])

        # 6) BBMD optional
        if self.cfg.get("bbmd_ip"):
            reg = getattr(self.app, "register_bbmd", None)
            if reg:
                try:
                    await maybe_await(reg(Address(self.cfg["bbmd_ip"]), self.cfg.get("bbmd_ttl", 600)))
                    LOG.info("Registered BBMD %s", self.cfg["bbmd_ip"])
                except Exception as ex:
                    LOG.warning("BBMD registration failed: %s", ex)

        # 7) Objekte anlegen
        for m in self.mappings:
            if m.object_type not in SUPPORTED_TYPES:
                LOG.warning("Unsupported type %s", m.object_type); continue
            key = await self._add_object(self.app, m, AnalogValueObject, BinaryValueObject, self.ha)
            self.objects[key] = m
            LOG.info("Added %s:%s -> %s", *key, m.entity_id)

    async def _on_state_changed(self, event):
        # Platzhalter für COV – derzeit on-demand via ReadProperty
        return

    async def run_forever(self):
        await self.start()
        stop = asyncio.Event()
        try:
            await stop.wait()
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
