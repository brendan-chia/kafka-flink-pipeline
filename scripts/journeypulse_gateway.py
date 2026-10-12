"""Run the JourneyPulse gateway; host default is loopback port 8020."""
import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from journeypulse.gateway import create_app
import uvicorn


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8020)
    args = parser.parse_args()
    # One process owns the local rate budgets and metrics.
    uvicorn.run(create_app(), host=args.host, port=args.port,
                timeout_keep_alive=5, limit_concurrency=64)


if __name__ == '__main__':
    main()
