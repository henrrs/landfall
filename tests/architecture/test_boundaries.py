import ast
from pathlib import Path


def test_core_import_boundary():
    core = Path(__file__).parents[2] / "src" / "landfall" / "core"
    banned = ("landfall.adapters", "mssql", "pyspark", "azure", "databricks", "docker")
    for file in core.rglob("*.py"):
        tree = ast.parse(file.read_text())
        imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        imports += [alias.name for n in ast.walk(tree) if isinstance(n, ast.Import) for alias in n.names]
        assert all(not name.startswith(banned) for name in imports), file


def test_contract_has_no_platform_config_reference():
    contract = Path(__file__).parents[2] / "src" / "landfall" / "core" / "contract.py"
    assert "PlatformConfig" not in contract.read_text()
