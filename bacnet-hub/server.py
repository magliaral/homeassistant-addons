"""
FastAPI-basierter einfacher RPC-Server für Home Assistant Add-on

Liest Konfiguration aus /config/bacnet-hub/mappings.yaml:
  server:
    host: 0.0.0.0
    port: 8000
    log_level: info
  bacpypes:
    # EINE von beiden Varianten:
    # 1) Liste der BACpypes-CLI-Argumente (roh)
    args:
      - --device-instance
      - "1234"
      - --address
      - "0.0.0.0"
    # 2) Oder als Dictionary (wird zu Flags konvertiert)
    # options:
    #   device_instance: 1234
    #   address: "0.0.0.0"
    #   no_who_is: true   # bool -> nur Flag, wenn True

Erforderliche Pakete:
  fastapi
  uvicorn[standard]
  pyyaml
  bacpypes3
"""

from __future__ import annotations

import os
import sys
import asyncio
import argparse
from contextlib import asynccontextmanager
from typing import Optional, Any, Dict, List, Union

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from bacpypes3.debugging import ModuleLogger
from bacpypes3.argparse import SimpleArgumentParser

from bacpypes3.pdu import Address, GlobalBroadcast
from bacpypes3.primitivedata import Atomic, ObjectIdentifier
from bacpypes3.constructeddata import Sequence, AnyAtomic, Array, List
from bacpypes3.apdu import ErrorRejectAbortNack
from bacpypes3.app import Application

from bacpypes3.settings import settings
from bacpypes3.json.util import (
    atomic_encode,
    sequence_to_json,
    extendedlist_to_json_list,
)

# Debugging
_debug = 0
_log = ModuleLogger(globals())

# Globale Variablen
args: argparse.Namespace
service: Application

# Pfad zur HA-Konfiguration
DEFAULT_CONFIG_PATH = "/config/bacnet-hub/mappings.yaml"


def _load_yaml_config(path: str) -> Dict[str, Any]:
    """
    YAML-Konfiguration vom Datenträger laden.
    Gibt ein leeres Dict zurück, wenn Datei fehlt oder leer ist.
    """
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
    """
    Dict -> CLI-Argumentliste (z.B. {'device_instance': 123, 'no_who_is': True}
    wird zu ['--device-instance', '123', '--no-who-is']).
    """
    argv: List[str] = []
    for key, value in options.items():
        flag = f"--{str(key).replace('_', '-')}"
        # Bool: True -> nur Flag setzen, False -> weglassen
        if isinstance(value, bool):
            if value:
                argv.append(flag)
        # None -> ignorieren
        elif value is None:
            continue
        # Liste/Tuple -> mehrfach setzen
        elif isinstance(value, (list, tuple)):
            for item in value:
                argv.extend([flag, str(item)])
        else:
            argv.extend([flag, str(value)])
    return argv


def _build_argv_from_yaml(config: Dict[str, Any]) -> List[str]:
    """
    Erzeuge eine vollständige argv-Liste aus YAML:
    - BACpypes-Argumente (aus 'bacpypes.args' oder 'bacpypes.options')
    - Server-Argumente (--host, --port, --log-level)
    """
    argv: List[str] = []

    # BACpypes
    bac = config.get("bacpypes", {}) or {}
    if "args" in bac and isinstance(bac["args"], list):
        argv.extend([str(x) for x in bac["args"]])
    elif "options" in bac and isinstance(bac["options"], dict):
        argv.extend(_bacpypes_dict_to_argv(bac["options"]))

    # Server
    server_cfg = config.get("server", {}) or {}
    host = str(server_cfg.get("host", os.getenv("BACNET_HUB_HOST", "0.0.0.0")))
    port = int(server_cfg.get("port", os.getenv("BACNET_HUB_PORT", "8000")))
    log_level = str(server_cfg.get("log_level", os.getenv("BACNET_HUB_LOG_LEVEL", "info")))

    argv.extend(["--host", host, "--port", str(port), "--log-level", log_level])

    if _debug:
        _log.debug("Finale argv aus YAML: %r", argv)
    return argv


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI-Lebenszyklus: baut die BACpypes Application aus den geparsten Argumenten.
    """
    global args, service
    service = Application.from_args(args)
    if _debug:
        _log.debug("lifespan service: %r", service)
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    """Zur API-Doku umleiten."""
    return RedirectResponse("/docs")


@app.get("/config")
async def get_config():
    """
    Konfiguration als JSON zurückgeben (BACpypes settings + Application-Objekte).
    """
    global service

    object_list = []
    for obj in service.objectIdentifier.values():
        object_list.append(sequence_to_json(obj))

    return {"BACpypes": dict(settings), "application": object_list}


@app.get("/{device_instance}")
async def who_is(device_instance: int, address: Optional[str] = None):
    """
    Who-Is senden und I-Am-Antworten zurückgeben.
    """
    global service

    destination: Address = Address(address) if address else GlobalBroadcast()
    i_ams = await service.who_is(device_instance, device_instance, destination)

    return [sequence_to_json(i_am) for i_am in i_ams]


async def _read_property(
    device_instance: int, object_identifier: str, property_identifier: str
):
    """
    Eine Eigenschaft eines Objekts lesen.
    """
    global service

    device_info = service.device_info_cache.instance_cache.get(device_instance, None)
    if device_info:
        device_address = device_info.device_address
    else:
        i_ams = await service.who_is(device_instance, device_instance)
        if not i_ams:
            raise HTTPException(status_code=400, detail=f"device not found: {device_instance}")
        if len(i_ams) > 1:
            raise HTTPException(status_code=400, detail=f"multiple devices: {device_instance}")
        device_address = i_ams[0].pduSource

    try:
        property_value = await service.read_property(
            device_address, ObjectIdentifier(object_identifier), property_identifier
        )
    except ErrorRejectAbortNack as err:
        raise HTTPException(status_code=400, detail=f"error/reject/abort: {err}")

    if isinstance(property_value, AnyAtomic):
        property_value = property_value.get_value()

    if isinstance(property_value, Atomic):
        encoded_value = atomic_encode(property_value)
    elif isinstance(property_value, Sequence):
        encoded_value = sequence_to_json(property_value)
    elif isinstance(property_value, (Array, List)):
        encoded_value = extendedlist_to_json_list(property_value)
    else:
        raise HTTPException(status_code=400, detail=f"JSON encoding: {property_value}")

    return {property_identifier: encoded_value}


@app.get("/{device_instance}/{object_identifier}")
async def read_present_value(device_instance: int, object_identifier: str):
    """present-value lesen."""
    return await _read_property(device_instance, object_identifier, "present-value")


@app.get("/{device_instance}/{object_identifier}/{property_identifier}")
async def read_property(
    device_instance: int, object_identifier: str, property_identifier: str
):
    """Beliebige Eigenschaft lesen."""
    return await _read_property(device_instance, object_identifier, property_identifier)


async def main() -> None:
    """
    Startpunkt:
      - YAML laden
      - Argumente aus YAML -> argparse
      - FastAPI/Uvicorn starten
    """
    global args, app

    # YAML-Pfad kann optional via ENV überschrieben werden
    cfg_path = os.getenv("BACNET_HUB_CONFIG", DEFAULT_CONFIG_PATH)
    cfg = _load_yaml_config(cfg_path)

    # Argumentparser aufbauen (inkl. unserer Server-Flags)
    parser = SimpleArgumentParser()
    parser.add_argument("--host", help="Host-Adresse für den Webservice", default="0.0.0.0")
    parser.add_argument("--port", type=int, help="Port für den Webservice", default=8000)
    parser.add_argument("--log-level", help="uvicorn log level", default="info")

    # argv aus YAML generieren und parsen
    yaml_argv = _build_argv_from_yaml(cfg)
    # Hinweis: Wir parsen NICHT aus sys.argv, sondern rein aus YAML.
    args = parser.parse_args(yaml_argv)

    if _debug:
        _log.debug("Args nach parse_args(yaml_argv): %r", args)

    # Uvicorn starten
    config = uvicorn.Config(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    # Für HA-Add-on: einfach python3 server.py
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
