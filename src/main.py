"""SEC EDGAR connector - dynamically discovers and runs all nodes."""
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

# SEC requires email in User-Agent per their fair access policy
if 'HTTP_USER_AGENT' not in os.environ:
    os.environ['HTTP_USER_AGENT'] = os.getenv('SEC_USER_AGENT', 'DataIntegrations/1.0 (admin@dataintegrations.io)')

from subsets_utils import load_nodes, validate_environment


def main():
    validate_environment(["SEC_USER_AGENT"])
    workflow = load_nodes()
    workflow.run()


if __name__ == "__main__":
    main()
