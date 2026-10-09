"""Start the standalone API; loopback-only by default."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import uvicorn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',default='127.0.0.1')
    parser.add_argument('--port',type=int,default=8010)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error('--port must be in 1..65535')
    uvicorn.run('datalens.api:app',host=args.host,port=args.port,
                limit_concurrency=32,timeout_keep_alive=5)


if __name__ == '__main__':
    main()
