"""Proxy-compatible alternative to Ivy resolution; preserve TLS and checksum checks."""
import hashlib
from pathlib import Path
from urllib.request import urlopen

root = Path('.cache/jars')
root.mkdir(parents=True, exist_ok=True)
for artifact in ['delta-spark_2.13', 'delta-storage']:
    name = f'{artifact}-4.0.0.jar'
    url = f'https://repo.maven.apache.org/maven2/io/delta/{artifact}/4.0.0/{name}'
    data = urlopen(url, timeout=60).read()
    expected = urlopen(url + '.sha1', timeout=60).read().decode().strip()
    if hashlib.sha1(data).hexdigest() != expected:
        raise RuntimeError(f'Checksum mismatch: {name}')
    (root / name).write_bytes(data)
    print(f'Verified {name}')
