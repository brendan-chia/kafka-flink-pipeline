"""Download pinned dependencies and verify the strongest published Maven digest."""
import hashlib
from pathlib import Path
import urllib.request
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = [
    ('org/apache/iceberg', 'iceberg-flink-runtime-2.2', '1.12.0'),
    ('org/apache/iceberg', 'iceberg-aws-bundle', '1.12.0'),
    ('org/apache/hadoop', 'hadoop-client-api', '3.3.6'),
    ('org/apache/hadoop', 'hadoop-client-runtime', '3.3.6'),
    ('commons-logging', 'commons-logging', '1.2'),
]


def download():
    directory = ROOT / 'jars'
    directory.mkdir(exist_ok=True)
    for group, artifact, version in ARTIFACTS:
        filename = f'{artifact}-{version}.jar'
        url = f'https://repo.maven.apache.org/maven2/{group}/{artifact}/{version}/{filename}'
        destination = directory / filename
        for algorithm in ('sha512', 'sha256', 'sha1'):
            try:
                digest = urllib.request.urlopen(url + '.' + algorithm, timeout=30).read().decode().split()[0]
                break
            except HTTPError as error:
                if error.code != 404:
                    raise
        else:
            raise RuntimeError(f'No published checksum: {url}')
        if destination.exists() and hashlib.new(algorithm, destination.read_bytes()).hexdigest() == digest:
            print(f'Verified cached {filename}')
            continue
        temporary = destination.with_suffix('.jar.part')
        print(f'Downloading {filename}', flush=True)
        with urllib.request.urlopen(url, timeout=60) as source, temporary.open('wb') as output:
            while block := source.read(1024 * 1024):
                output.write(block)
        if hashlib.new(algorithm, temporary.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f'{algorithm} mismatch: {temporary}')
        temporary.replace(destination)
        print(f'Verified {filename} ({algorithm})', flush=True)


if __name__ == '__main__':
    download()
