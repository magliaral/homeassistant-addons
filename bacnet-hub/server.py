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
