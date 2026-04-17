"""SEC EDGAR connector - dynamically discovers and runs all nodes."""
from subsets_utils import load_nodes, validate_environment


def main():
    validate_environment(["SEC_USER_AGENT"])
    workflow = load_nodes()
    workflow.run()


if __name__ == "__main__":
    main()
