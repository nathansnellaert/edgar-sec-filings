"""Dynamic node discovery and execution.

Scans a nodes/ directory at runtime and collects all NODES dicts,
builds a DAG, and executes sequentially.

Usage:
    from subsets_utils.nodes import load_nodes

    # In main.py:
    workflow = load_nodes()
    workflow.run()

    # Or with explicit path:
    workflow = load_nodes("src/nodes")
    workflow.run()
"""

import importlib.util
import sys
from pathlib import Path
from typing import Callable

from .dag import DAG
from .environment import validate_environment


def load_nodes(nodes_dir: Path | str | None = None) -> DAG:
    """Discover nodes and return a DAG ready to run.

    Scans the nodes directory for .py files, loads each module dynamically,
    and collects their NODES dicts (download -> transform mappings).
    Returns a DAG with download -> transform dependencies.

    Args:
        nodes_dir: Path to nodes directory. Defaults to src/nodes relative to cwd.

    Returns:
        DAG instance ready to .run()
    """
    validate_environment()

    if nodes_dir is None:
        nodes_dir = Path.cwd() / "src" / "nodes"
    elif isinstance(nodes_dir, str):
        nodes_dir = Path(nodes_dir)

    print(f"Loading nodes from: {nodes_dir}")

    all_nodes: dict[Callable, list[Callable]] = {}

    if not nodes_dir.exists():
        print(f"Warning: nodes directory not found: {nodes_dir}")
        return DAG(all_nodes)

    # Find all .py files in nodes directory (not __init__.py, not _prefixed)
    node_files = sorted(nodes_dir.glob("*.py"))

    for node_file in node_files:
        if node_file.name.startswith("_"):
            continue

        module_name = f"nodes.{node_file.stem}"

        try:
            # Load module dynamically
            spec = importlib.util.spec_from_file_location(module_name, node_file)
            if spec is None or spec.loader is None:
                print(f"Warning: Could not load spec for {node_file}")
                continue

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Extract NODES dict if present
            if hasattr(module, "NODES"):
                nodes_dict = getattr(module, "NODES")
                if isinstance(nodes_dict, dict):
                    # Convert {download: transform} to DAG format {download: [], transform: [download]}
                    for download_fn, transform_fn in nodes_dict.items():
                        all_nodes[download_fn] = []
                        all_nodes[transform_fn] = [download_fn]

        except Exception as e:
            print(f"Error loading {node_file.name}: {e}")
            raise

    print(f"Loaded {len(all_nodes) // 2} nodes")
    return DAG(all_nodes)
