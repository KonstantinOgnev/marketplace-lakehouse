# Marketplace Lakehouse CI/CD

Stage 15 baseline validated in Azure DevOps: protected `main`, secret-free PR checks,
Databricks Asset Bundle validation, dev deployment, and service-principal smoke test.

Minimal Azure DevOps and Databricks Declarative Automation Bundle project for
the Marketplace Lakehouse laboratory.

## Repository layout

- `src/` — reusable Python code that can be unit-tested without Databricks.
- `notebooks/` — deployable Databricks source notebooks.
- `tests/` — unit and structural tests.
- `resources/` — Bundle resource definitions.
- `config/` — non-secret environment configuration.
- `docs/` — architecture and operating documentation.
- `scripts/` — local and CI validation commands.

## Local quality checks

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pip install -e .
ruff check src tests scripts
python scripts/validate_repo.py
pytest -q
```

## Required Azure DevOps secret variables

Create a protected variable group and map these names:

- `DATABRICKS_HOST` — Azure Databricks workspace URL.
- `DATABRICKS_CLIENT_ID` — application ID of the CI/CD service principal.
- `DATABRICKS_CLIENT_SECRET` — secret value; mark it secret.

The pipeline uses Databricks OAuth M2M unified authentication. Never commit
these values or a `.databrickscfg` file.

## Bundle commands

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run marketplace_smoke_test -t dev
```

The initial bundle deliberately deploys only an isolated smoke-test Job. The
existing `marketplace_lakehouse_daily` Job will be migrated and bound in a later
step after the CI quality gate has passed.
