"""Tests for configuration files (pre-commit, pyright, etc.)."""

import json
import pathlib

import pytest
import yaml


@pytest.fixture
def repo_root() -> pathlib.Path:
    """Get repository root directory."""
    return pathlib.Path(__file__).parent.parent.parent


@pytest.fixture
def precommit_config(repo_root: pathlib.Path) -> dict:
    """Load pre-commit configuration."""
    config_path = repo_root / ".pre-commit-config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def pyright_config(repo_root: pathlib.Path) -> dict:
    """Load pyright configuration."""
    config_path = repo_root / "pyrightconfig.json"
    with open(config_path) as f:
        return json.load(f)


def test_precommit_config_structure(precommit_config: dict):
    """Test pre-commit config has required structure."""
    assert "repos" in precommit_config
    assert isinstance(precommit_config["repos"], list)
    assert len(precommit_config["repos"]) > 0


def test_precommit_has_essential_hooks(precommit_config: dict):
    """Test pre-commit config has essential hooks."""
    all_hook_ids = []
    for repo in precommit_config["repos"]:
        if "hooks" in repo:
            all_hook_ids.extend([hook["id"] for hook in repo["hooks"]])

    # Essential hooks
    assert "check-yaml" in all_hook_ids
    assert "check-json" in all_hook_ids
    assert "check-ast" in all_hook_ids
    assert "trailing-whitespace" in all_hook_ids
    assert "end-of-file-fixer" in all_hook_ids
    assert "check-merge-conflict" in all_hook_ids


def test_precommit_has_python_quality_tools(precommit_config: dict):
    """Test pre-commit config has Python quality tools."""
    all_hook_ids = []
    for repo in precommit_config["repos"]:
        if "hooks" in repo:
            all_hook_ids.extend([hook["id"] for hook in repo["hooks"]])

    # Python quality tools
    assert "ruff" in all_hook_ids
    assert "ruff-format" in all_hook_ids
    assert "pyright" in all_hook_ids
    assert "pycln" in all_hook_ids
    assert "vermin" in all_hook_ids


def test_precommit_has_test_hooks(precommit_config: dict):
    """Test pre-commit config runs tests."""
    all_hook_ids = []
    for repo in precommit_config["repos"]:
        if "hooks" in repo:
            all_hook_ids.extend([hook["id"] for hook in repo["hooks"]])

    assert "pytest-unit" in all_hook_ids
    assert "pytest-integration" in all_hook_ids


def test_precommit_ruff_config(precommit_config: dict):
    """Test ruff hooks are properly configured."""
    ruff_repo = None
    for repo in precommit_config["repos"]:
        if "ruff-pre-commit" in repo.get("repo", ""):
            ruff_repo = repo
            break

    assert ruff_repo is not None
    assert "hooks" in ruff_repo

    hook_ids = [hook["id"] for hook in ruff_repo["hooks"]]
    assert "ruff" in hook_ids
    assert "ruff-format" in hook_ids

    # Check ruff has fix flag and config
    for hook in ruff_repo["hooks"]:
        if hook["id"] == "ruff":
            assert "--fix" in hook["args"]
            assert "--config" in hook["args"]
            assert "ruff.toml" in hook["args"]


def test_precommit_pyright_config(precommit_config: dict):
    """Test pyright hook is properly configured."""
    pyright_repo = None
    for repo in precommit_config["repos"]:
        if "pyright-python" in repo.get("repo", ""):
            pyright_repo = repo
            break

    assert pyright_repo is not None
    hooks = pyright_repo["hooks"]
    assert len(hooks) == 1
    assert hooks[0]["id"] == "pyright"
    assert hooks[0]["types"] == ["python"]
    assert "--project" in hooks[0]["args"]


def test_precommit_vermin_target_version(precommit_config: dict):
    """Test vermin checks correct Python version."""
    vermin_repo = None
    for repo in precommit_config["repos"]:
        if "vermin" in repo.get("repo", ""):
            vermin_repo = repo
            break

    assert vermin_repo is not None
    hook = vermin_repo["hooks"][0]
    assert hook["id"] == "vermin"

    # Check target version is set to 3.10
    assert "--target=3.10-" in hook["args"]


def test_precommit_pytest_hooks_config(precommit_config: dict):
    """Test pytest hooks are properly configured."""
    pytest_hooks = []
    for repo in precommit_config["repos"]:
        if repo.get("repo") == "local":
            for hook in repo.get("hooks", []):
                if hook["id"].startswith("pytest-"):
                    pytest_hooks.append(hook)

    assert len(pytest_hooks) >= 2

    # Check unit tests hook
    unit_hook = next((h for h in pytest_hooks if h["id"] == "pytest-unit"), None)
    assert unit_hook is not None
    assert unit_hook["language"] == "system"
    assert unit_hook["types"] == ["python"]
    assert unit_hook["pass_filenames"] is False
    assert unit_hook["always_run"] is True
    assert "./tests/unit" in unit_hook["entry"]

    # Check integration tests hook
    integration_hook = next((h for h in pytest_hooks if h["id"] == "pytest-integration"), None)
    assert integration_hook is not None
    assert integration_hook["language"] == "system"
    assert integration_hook["types"] == ["python"]
    assert integration_hook["pass_filenames"] is False
    assert integration_hook["always_run"] is True
    assert "./tests/integration" in integration_hook["entry"]


def test_precommit_exclude_patterns(precommit_config: dict):
    """Test pre-commit has proper exclude patterns."""
    # Find hooks with exclude patterns
    hooks_with_excludes = []
    for repo in precommit_config["repos"]:
        for hook in repo.get("hooks", []):
            if "exclude" in hook:
                hooks_with_excludes.append(hook)

    assert len(hooks_with_excludes) > 0

    # Check that test directories and fixtures are excluded where appropriate
    for hook in hooks_with_excludes:
        exclude_pattern = hook["exclude"]
        if hook["id"] in ["trailing-whitespace", "name-tests-test"]:
            assert "tests/mock/" in exclude_pattern or "^tests/mock/" in exclude_pattern
            assert "tests/integration/" in exclude_pattern or "^tests/integration/" in exclude_pattern


def test_pyright_config_structure(pyright_config: dict):
    """Test pyright config has required structure."""
    assert "venvPath" in pyright_config
    assert "venv" in pyright_config
    assert "include" in pyright_config
    assert "exclude" in pyright_config
    assert "pythonVersion" in pyright_config
    assert "pythonPlatform" in pyright_config


def test_pyright_include_paths(pyright_config: dict):
    """Test pyright includes correct paths."""
    include = pyright_config["include"]
    assert "src" in include
    assert "tests" in include
    assert "benchmarks" in include


def test_pyright_exclude_patterns(pyright_config: dict):
    """Test pyright excludes correct patterns."""
    exclude = pyright_config["exclude"]
    assert "**/__pycache__" in exclude
    assert "venv" in exclude
    assert "**/*_pb2.py" in exclude  # Exclude protobuf generated files


def test_pyright_python_version(pyright_config: dict):
    """Test pyright targets correct Python version."""
    assert pyright_config["pythonVersion"] == "3.10"
    assert pyright_config["pythonPlatform"] == "Linux"


def test_pyright_import_settings(pyright_config: dict):
    """Test pyright import reporting settings."""
    assert pyright_config["reportMissingImports"] == "error"
    assert pyright_config["reportMissingTypeStubs"] is False


def test_pyright_execution_environments(pyright_config: dict):
    """Test pyright execution environments."""
    assert "executionEnvironments" in pyright_config
    envs = pyright_config["executionEnvironments"]
    assert len(envs) >= 2

    # Check main environment
    main_env = next((e for e in envs if e["root"] == "./"), None)
    assert main_env is not None
    assert main_env["pythonVersion"] == "3.10"
    assert main_env["pythonPlatform"] == "Linux"
    assert main_env["reportMissingImports"] == "error"

    # Check examples environment
    examples_env = next((e for e in envs if e["root"] == "./examples"), None)
    assert examples_env is not None
    assert examples_env["pythonVersion"] == "3.10"
    assert examples_env["reportMissingImports"] == "warning"  # More lenient for examples


def test_pyright_define_constants(pyright_config: dict):
    """Test pyright defines constants."""
    assert "defineConstant" in pyright_config
    assert pyright_config["defineConstant"]["DEBUG"] is True


def test_precommit_repo_versions_format(precommit_config: dict):
    """Test pre-commit repo versions are properly specified."""
    for repo in precommit_config["repos"]:
        if repo.get("repo") != "local":
            assert "rev" in repo, f"Repository {repo.get('repo')} missing 'rev' field"
            rev = repo["rev"]
            # Should start with v or be a valid version
            assert isinstance(rev, str) and len(rev) > 0


def test_precommit_trailing_comma_hook(precommit_config: dict):
    """Test add-trailing-comma hook is present."""
    trailing_comma_repo = None
    for repo in precommit_config["repos"]:
        if "add-trailing-comma" in repo.get("repo", ""):
            trailing_comma_repo = repo
            break

    assert trailing_comma_repo is not None
    hooks = trailing_comma_repo["hooks"]
    assert any(hook["id"] == "add-trailing-comma" for hook in hooks)


def test_precommit_type_hints_upgrade(precommit_config: dict):
    """Test upgrade-type-hints hook is present."""
    type_hints_repo = None
    for repo in precommit_config["repos"]:
        if "pep585-upgrade" in repo.get("repo", ""):
            type_hints_repo = repo
            break

    assert type_hints_repo is not None
    hooks = type_hints_repo["hooks"]
    assert any(hook["id"] == "upgrade-type-hints" for hook in hooks)


def test_config_files_exist(repo_root: pathlib.Path):
    """Test that all expected config files exist."""
    assert (repo_root / ".pre-commit-config.yaml").exists()
    assert (repo_root / "pyrightconfig.json").exists()
    assert (repo_root / ".github" / "workflows" / "codspeed.yml").exists()
    assert (repo_root / ".github" / "workflows" / "tests.yml").exists()
    assert (repo_root / "pyproject.toml").exists()
    assert (repo_root / "tests" / "pytest-config.ini").exists()


def test_pyright_config_is_valid_json(repo_root: pathlib.Path):
    """Test pyrightconfig.json is valid JSON."""
    config_path = repo_root / "pyrightconfig.json"
    with open(config_path) as f:
        content = f.read()
        # Should not raise exception
        json.loads(content)


def test_precommit_config_is_valid_yaml(repo_root: pathlib.Path):
    """Test .pre-commit-config.yaml is valid YAML."""
    config_path = repo_root / ".pre-commit-config.yaml"
    with open(config_path) as f:
        content = f.read()
        # Should not raise exception
        yaml.safe_load(content)