#!/usr/bin/env python3
"""Forward local development connections through the Shuttle server."""
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit

root = Path(__file__).resolve().parents[1]
config = dict(
    line.split("=", 1)
    for line in (root / ".env").read_text().splitlines()
    if line and not line.startswith("#") and "=" in line
)
postgres = urlsplit(config["POSTGRES_URL"])
redis = urlsplit(config["REDIS_URL"])
hub_host, hub_port = config["HUB_HOST"].rsplit(":", 1)
forwards = [
    (15432, postgres.hostname, postgres.port or 5432),
    (26379, redis.hostname, redis.port or 6379),
    (23383, hub_host, int(hub_port)),
    (23381, hub_host, 3381),
]
args = [
    "ssh", "-N", "-i", os.environ.get("SHUTTLE_SSH_IDENTITY", str(Path.home() / "Project/ssh/tencent-shuttle")),
    "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
    "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
]
for local_port, host, port in forwards:
    args.extend(["-L", f"127.0.0.1:{local_port}:{host}:{port}"])
if "--print" in sys.argv:
    for local_port, host, port in forwards:
        print(f"127.0.0.1:{local_port} -> {host}:{port}")
else:
    args.append(os.environ.get("SHUTTLE_SSH_HOST", "ubuntu@43.130.67.184"))
    os.execvp("ssh", args)
