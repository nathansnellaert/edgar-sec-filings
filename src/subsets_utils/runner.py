#!/usr/bin/env python3
"""Minimal supervisor for OOM resilience. Runs src.main in subprocess, uploads logs on crash."""

import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .config import is_cloud
from .r2 import upload_file


def main():
    run_id = datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d-%H%M%S')
    log_dir = Path("/tmp/logs" if is_cloud() else "logs") / run_id
    log_dir.mkdir(parents=True, exist_ok=True)

    env = {**__import__('os').environ, 'LOG_DIR': str(log_dir), 'RUN_ID': run_id}
    # Add src to PYTHONPATH so 'from subsets_utils import' works in main.py
    src_path = str(Path.cwd() / 'src')
    existing_pythonpath = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = f"{src_path}:{existing_pythonpath}" if existing_pythonpath else src_path

    with open(log_dir / 'output.log', 'w') as f:
        proc = subprocess.Popen([sys.executable, '-m', 'src.main'], stdout=f, stderr=subprocess.STDOUT, env=env, cwd=Path.cwd())
        exit_code = proc.wait()

    if exit_code != 0:
        print(f"Failed with exit code {exit_code}" + (" (OOM)" if exit_code == 137 else ""))

    if is_cloud():
        for log in log_dir.rglob('*'):
            if log.is_file():
                upload_file(str(log), f"{Path.cwd().name}/logs/{run_id}/{log.relative_to(log_dir)}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
