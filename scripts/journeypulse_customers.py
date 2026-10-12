"""Run the standalone customer API on loopback."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import uvicorn
from journeypulse.customer_api import create_app

if __name__ == '__main__':
    uvicorn.run(create_app(), host='127.0.0.1', port=8021)
