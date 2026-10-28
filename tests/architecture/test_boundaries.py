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


def test_planner_contains_no_sql_compiler_or_source_adapter_import():
    planner = Path(__file__).parents[2] / "src" / "landfall" / "core" / "planner.py"
    tree = ast.parse(planner.read_text())
    imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert all("sqlserver" not in name and "adapters" not in name for name in imports)
    assert "SELECT " not in planner.read_text()


def test_application_depends_on_ports_not_concrete_adapters():
    application = Path(__file__).parents[2] / "src" / "landfall" / "application"
    for file in application.rglob("*.py"):
        tree = ast.parse(file.read_text())
        imports = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
        assert all(not name.startswith("landfall.adapters") for name in imports), file


def test_sqlserver_adapter_satisfies_source_port_surface():
    from landfall.adapters.sqlserver import SqlServerConnector

    assert all(callable(getattr(SqlServerConnector, method, None)) for method in
               ("validate", "profile", "high_position", "read"))


def test_production_source_has_no_jdbc_path():
    source_root = Path(__file__).parents[2] / "src" / "landfall"
    forbidden = (
        "spark.read.jdbc(", "dataframereader.jdbc(", 'format("jdbc")',
        "jdbc:sqlserver://", "com.microsoft.sqlserver.jdbc.", "java.sql.",
    )
    for file in source_root.rglob("*.py"):
        source = file.read_text().lower()
        assert not [entry for entry in forbidden if entry in source], file


def test_pyspark_imports_are_confined_to_runtime_adapters():
    source_root = Path(__file__).parents[2] / "src" / "landfall"
    for file in source_root.rglob("*.py"):
        tree = ast.parse(file.read_text())
        imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        imports += [alias.name for node in ast.walk(tree) if isinstance(node, ast.Import)
                    for alias in node.names]
        if any(name.startswith(("pyspark", "py4j")) for name in imports):
            assert file.parts[-2] == "adapters", file
