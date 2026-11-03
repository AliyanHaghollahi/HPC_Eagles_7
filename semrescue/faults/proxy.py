"""TCP fault-injection proxy that sits between the glasses (coordinator) and one tier.

Modes:
  pass       forward traffic, adding the emulated link delay
  partition  the tier is alive but unreachable: connections are accepted and data is
             silently dropped, so the client only notices through a timeout
             ("edge available but we cannot connect to it")
  reject     connections are refused/closed immediately (fast failure, like a dead host
             that answers with a TCP reset)

A crashed tier ("edge not available") is produced by killing the tier process; the
proxy then gets 'connection refused' from the target and closes the client socket.

Usage as a library (experiments) or standalone:
  python -m semrescue.faults.proxy --listen 9001 --target 192.168.1.50:8001 --control 9101
  curl -X POST "localhost:9101/mode?mode=partition"
"""
from __future__ import annotations

import argparse
import asyncio


class FaultProxy:
    def __init__(self, listen_port: int, target_host: str, target_port: int,
                 delay_ms: float = 0.0, listen_host: str = "127.0.0.1"):
        self.listen_host, self.listen_port = listen_host, listen_port
        self.target_host, self.target_port = target_host, target_port
        self.delay_s = delay_ms / 1000.0
        self.mode = "pass"
        self._server: asyncio.base_events.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.listen_host, self.listen_port)

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            self.drop_connections()
            await self._server.wait_closed()

    def set_mode(self, mode: str) -> None:
        assert mode in ("pass", "partition", "reject"), mode
        self.mode = mode
        if mode == "reject":
            self.drop_connections()

    def drop_connections(self) -> None:
        for w in list(self._writers):
            w.close()
        self._writers.clear()

    async def _handle(self, c_reader: asyncio.StreamReader, c_writer: asyncio.StreamWriter):
        if self.mode == "reject":
            c_writer.close()
            return
        self._writers.add(c_writer)
        try:
            t_reader, t_writer = await asyncio.open_connection(self.target_host, self.target_port)
        except OSError:
            # target process is down -> behave like the host refused the connection
            self._writers.discard(c_writer)
            c_writer.close()
            return
        self._writers.add(t_writer)
        await asyncio.gather(self._pipe(c_reader, t_writer), self._pipe(t_reader, c_writer),
                             return_exceptions=True)
        for w in (c_writer, t_writer):
            self._writers.discard(w)
            w.close()

    async def _pipe(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if self.mode == "partition":
                continue  # black hole: drop silently, the peer waits until it times out
            if self.mode == "reject":
                break
            if self.delay_s:
                await asyncio.sleep(self.delay_s)
            writer.write(data)
            await writer.drain()
        try:
            writer.write_eof()
        except (OSError, RuntimeError, AttributeError):
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, required=True)
    ap.add_argument("--listen-host", default="127.0.0.1")
    ap.add_argument("--target", required=True, help="host:port of the tier")
    ap.add_argument("--delay-ms", type=float, default=0)
    ap.add_argument("--control", type=int, default=None, help="HTTP control port")
    args = ap.parse_args()
    host, port = args.target.rsplit(":", 1)
    proxy = FaultProxy(args.listen, host, int(port), args.delay_ms, args.listen_host)

    async def run():
        await proxy.start()
        print(f"proxy {args.listen_host}:{args.listen} -> {args.target} (mode={proxy.mode})")
        if args.control:
            import uvicorn
            from fastapi import FastAPI
            app = FastAPI()

            @app.post("/mode")
            async def set_mode(mode: str):
                proxy.set_mode(mode)
                return {"mode": proxy.mode}

            @app.get("/mode")
            async def get_mode():
                return {"mode": proxy.mode}

            server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.control, log_level="warning"))
            await server.serve()
        else:
            await asyncio.Event().wait()

    asyncio.run(run())


if __name__ == "__main__":
    main()
