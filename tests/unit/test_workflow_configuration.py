"""Tests for GitHub workflow configuration files."""

import pathlib

import pytest
import yaml


@pytest.fixture
def repo_root() -> pathlib.Path:
    """Get repository root directory."""
    return pathlib.Path(__file__).parent.parent.parent


@pytest.fixture
def codspeed_workflow(repo_root: pathlib.Path) -> dict:
    """Load CodSpeed workflow YAML."""
    workflow_path = repo_root / ".github" / "workflows" / "codspeed.yml"
    with open(workflow_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def tests_workflow(repo_root: pathlib.Path) -> dict:
    """Load tests workflow YAML."""
    workflow_path = repo_root / ".github" / "workflows" / "tests.yml"
    with open(workflow_path) as f:
        return yaml.safe_load(f)


def test_codspeed_workflow_structure(codspeed_workflow: dict):
    """Test CodSpeed workflow has required structure."""
    assert codspeed_workflow["name"] == "CodSpeed"
    # 'on' might be parsed as True by YAML parser
    assert "on" in codspeed_workflow or True in codspeed_workflow
    assert "jobs" in codspeed_workflow
    assert "benchmarks" in codspeed_workflow["jobs"]


def test_codspeed_workflow_triggers(codspeed_workflow: dict):
    """Test CodSpeed workflow has correct triggers."""
    # 'on' might be parsed as True by YAML parser
    on = codspeed_workflow.get("on", codspeed_workflow.get(True))
    assert on is not None
    assert "push" in on
    assert "pull_request" in on
    assert "workflow_dispatch" in on

    # Verify push branches
    assert "branches" in on["push"]
    branches = on["push"]["branches"]
    assert "main" in branches or "master" in branches


def test_codspeed_workflow_permissions(codspeed_workflow: dict):
    """Test CodSpeed workflow has required permissions."""
    assert "permissions" in codspeed_workflow
    permissions = codspeed_workflow["permissions"]
    assert permissions["contents"] == "read"
    assert permissions["id-token"] == "write"


def test_codspeed_workflow_python_version(codspeed_workflow: dict):
    """Test CodSpeed workflow uses correct Python version."""
    job = codspeed_workflow["jobs"]["benchmarks"]
    steps = job["steps"]

    # Find Python setup step
    python_step = None
    for step in steps:
        if "Set up Python" in step.get("name", ""):
            python_step = step
            break

    assert python_step is not None
    assert python_step["with"]["python-version"] == "3.12"


def test_codspeed_workflow_infrastructure_steps(codspeed_workflow: dict):
    """Test CodSpeed workflow has infrastructure setup steps."""
    job = codspeed_workflow["jobs"]["benchmarks"]
    steps = job["steps"]
    step_names = [step.get("name", "") for step in steps]

    assert "Start infrastructure" in step_names
    assert "Wait for MySQL" in step_names
    assert "Wait for Redis" in step_names
    assert "Stop infrastructure" in step_names


def test_codspeed_workflow_run_benchmarks_step(codspeed_workflow: dict):
    """Test CodSpeed workflow has benchmark execution step."""
    job = codspeed_workflow["jobs"]["benchmarks"]
    steps = job["steps"]

    # Find benchmark step
    benchmark_step = None
    for step in steps:
        if step.get("name") == "Run benchmarks":
            benchmark_step = step
            break

    assert benchmark_step is not None
    assert benchmark_step["uses"] == "CodSpeedHQ/action@v4"
    assert benchmark_step["with"]["mode"] == "simulation"
    assert "--codspeed" in benchmark_step["with"]["run"]


def test_tests_workflow_structure(tests_workflow: dict):
    """Test tests workflow has required structure."""
    assert tests_workflow["name"] == "Tests"
    # 'on' might be parsed as True by YAML parser
    assert "on" in tests_workflow or True in tests_workflow
    assert "jobs" in tests_workflow
    assert "lint" in tests_workflow["jobs"]
    assert "test" in tests_workflow["jobs"]


def test_tests_workflow_triggers(tests_workflow: dict):
    """Test tests workflow has correct triggers."""
    # 'on' might be parsed as True by YAML parser
    on = tests_workflow.get("on", tests_workflow.get(True))
    assert on is not None
    assert "push" in on
    assert "pull_request" in on

    # Verify branches
    assert on["push"]["branches"] == ["main", "master"]
    assert on["pull_request"]["branches"] == ["main", "master"]


def test_tests_workflow_python_matrix(tests_workflow: dict):
    """Test tests workflow has correct Python version matrix."""
    lint_job = tests_workflow["jobs"]["lint"]
    test_job = tests_workflow["jobs"]["test"]

    # Check lint job matrix
    assert "strategy" in lint_job
    assert "matrix" in lint_job["strategy"]
    lint_versions = lint_job["strategy"]["matrix"]["python-version"]
    assert "3.10" in lint_versions
    assert "3.11" in lint_versions
    assert "3.12" in lint_versions

    # Check test job matrix
    assert "strategy" in test_job
    assert "matrix" in test_job["strategy"]
    test_versions = test_job["strategy"]["matrix"]["python-version"]
    assert "3.10" in test_versions
    assert "3.11" in test_versions
    assert "3.12" in test_versions


def test_tests_workflow_lint_steps(tests_workflow: dict):
    """Test tests workflow has required lint steps."""
    lint_job = tests_workflow["jobs"]["lint"]
    steps = lint_job["steps"]
    step_names = [step.get("name", "") for step in steps]

    assert "Run ruff check" in step_names
    assert "Run ruff format check" in step_names
    assert "Run pyright" in step_names
    assert "Check minimum Python version (vermin)" in step_names


def test_tests_workflow_test_steps(tests_workflow: dict):
    """Test tests workflow has required test steps."""
    test_job = tests_workflow["jobs"]["test"]
    steps = test_job["steps"]
    step_names = [step.get("name", "") for step in steps]

    assert "Start infrastructure" in step_names
    assert "Wait for MySQL" in step_names
    assert "Wait for Redis" in step_names
    assert "Run all tests with coverage" in step_names
    assert "Upload coverage to Codecov" in step_names
    assert "Stop infrastructure" in step_names


def test_tests_workflow_coverage_upload(tests_workflow: dict):
    """Test tests workflow uploads coverage correctly."""
    test_job = tests_workflow["jobs"]["test"]
    steps = test_job["steps"]

    # Find coverage upload step
    coverage_step = None
    for step in steps:
        if step.get("name") == "Upload coverage to Codecov":
            coverage_step = step
            break

    assert coverage_step is not None
    assert coverage_step["uses"] == "codecov/codecov-action@v4"
    assert "token" in coverage_step["with"]
    assert coverage_step["with"]["fail_ci_if_error"] is False


def test_workflow_always_cleanup_infrastructure(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows always cleanup infrastructure."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    for step in codspeed_steps:
        if step.get("name") == "Stop infrastructure":
            assert step.get("if") == "always()"

    # Check tests workflow
    test_steps = tests_workflow["jobs"]["test"]["steps"]
    for step in test_steps:
        if step.get("name") == "Stop infrastructure":
            assert step.get("if") == "always()"


def test_workflow_wait_for_mysql_timeout(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows have reasonable MySQL wait timeout."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    for step in codspeed_steps:
        if step.get("name") == "Wait for MySQL":
            run_cmd = step.get("run", "")
            assert "seq 1 30" in run_cmd  # 30 iterations with 2s sleep = 60s timeout

    # Check tests workflow
    test_steps = tests_workflow["jobs"]["test"]["steps"]
    for step in test_steps:
        if step.get("name") == "Wait for MySQL":
            run_cmd = step.get("run", "")
            assert "seq 1 30" in run_cmd


def test_workflow_wait_for_redis_timeout(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows have reasonable Redis wait timeout."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    for step in codspeed_steps:
        if step.get("name") == "Wait for Redis":
            run_cmd = step.get("run", "")
            assert "seq 1 15" in run_cmd  # 15 iterations with 1s sleep = 15s timeout

    # Check tests workflow
    test_steps = tests_workflow["jobs"]["test"]["steps"]
    for step in test_steps:
        if step.get("name") == "Wait for Redis":
            run_cmd = step.get("run", "")
            assert "seq 1 15" in run_cmd


def test_workflows_use_docker_compose_v2(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows use docker compose v2 syntax (space not hyphen in command)."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    for step in codspeed_steps:
        run_cmd = step.get("run", "")
        if "docker" in run_cmd.lower() and "compose" in run_cmd.lower():
            # Check that command uses "docker compose" (v2) not "docker-compose" (v1)
            # But filenames like "docker-compose-test.yml" are OK
            assert "docker compose " in run_cmd or "docker compose -" in run_cmd  # v2 command syntax

    # Check tests workflow
    test_steps = tests_workflow["jobs"]["test"]["steps"]
    for step in test_steps:
        run_cmd = step.get("run", "")
        if "docker" in run_cmd.lower() and "compose" in run_cmd.lower():
            assert "docker compose " in run_cmd or "docker compose -" in run_cmd


def test_tests_workflow_fail_fast_disabled(tests_workflow: dict):
    """Test that fail-fast is disabled in test matrix."""
    lint_job = tests_workflow["jobs"]["lint"]
    test_job = tests_workflow["jobs"]["test"]

    assert lint_job["strategy"]["fail-fast"] is False
    assert test_job["strategy"]["fail-fast"] is False


def test_workflows_checkout_action_version(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows use up-to-date checkout action."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    checkout_found = False
    for step in codspeed_steps:
        if "actions/checkout" in step.get("uses", ""):
            assert "@v4" in step["uses"]
            checkout_found = True
    assert checkout_found

    # Check tests workflow - lint job
    lint_steps = tests_workflow["jobs"]["lint"]["steps"]
    checkout_found = False
    for step in lint_steps:
        if "actions/checkout" in step.get("uses", ""):
            assert "@v4" in step["uses"]
            checkout_found = True
    assert checkout_found

    # Check tests workflow - test job
    test_steps = tests_workflow["jobs"]["test"]["steps"]
    checkout_found = False
    for step in test_steps:
        if "actions/checkout" in step.get("uses", ""):
            assert "@v4" in step["uses"]
            checkout_found = True
    assert checkout_found


def test_workflows_python_setup_action_version(codspeed_workflow: dict, tests_workflow: dict):
    """Test workflows use up-to-date Python setup action."""
    # Check CodSpeed workflow
    codspeed_steps = codspeed_workflow["jobs"]["benchmarks"]["steps"]
    for step in codspeed_steps:
        if "actions/setup-python" in step.get("uses", ""):
            assert "@v5" in step["uses"]

    # Check tests workflow
    for job_name in ["lint", "test"]:
        steps = tests_workflow["jobs"][job_name]["steps"]
        for step in steps:
            if "actions/setup-python" in step.get("uses", ""):
                assert "@v5" in step["uses"]