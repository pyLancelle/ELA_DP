# Repository Guidelines

## Project Structure & Module Organization
- `src/`: Python package (Python 3.11)
  - `connectors/`: service integrations (e.g., `spotify/`, `garmin/`, `chess/`) and shared `utils.py`.
  - `services/mail/`: modular email service with CLI (`main.py`) and templates/providers.
  - `dbt_dataplatform/`: dbt project (`dbt_project.yml`, models under `models/`).
- `data/`: local data/exports.
- `ingestion-config-*.yaml`, `.env`, `gcs_key.json`: runtime configuration and credentials.
- `pyproject.toml`: dependencies and packaging; `.pre-commit-config.yaml`: formatting hooks.

## Build, Test, and Development Commands
- Environment: `uv sync` (preferred, uses `uv.lock`) or:
  - `python -m venv .venv && source .venv/bin/activate`
  - `pip install -e . && pre-commit install`
- Format/Lint: `pre-commit run -a` (runs Black).
- Mail CLI examples:
  - `python -m src.services.mail.main --perimeter spotify_weekly --destinataires you@example.com`
  - `python src/services/mail/example_usage.py`
- dbt: `dbt build --project-dir src/dbt_dataplatform --profiles-dir src/dbt_dataplatform`.

## Coding Style & Naming Conventions
- Use Black formatting (4-space indent, 88-char lines by default).
- Python: functions/variables `snake_case`, classes `PascalCase`, modules `snake_case`.
- Type hints encouraged; prefer small, testable functions.
- Place new connectors in `src/connectors/<service>/`; share config via `connectors/utils.get_settings()`.

## Testing Guidelines
- No formal test suite yet. Prefer adding `pytest`-based tests under `tests/` named `test_*.py`.
- For now, validate via module CLIs (mail service) and sample scripts in `services/mail/`.
- If adding dbt models, rely on `dbt build` (includes tests) and add schema tests where relevant.

## Commit & Pull Request Guidelines
- Commits: imperative, concise; prefer Conventional style (`feat:`, `fix:`, `chore:`, `docs:`).
- PRs: clear description, linked issues, screenshots/logs when UI/CLI output matters; list run steps.
- Before opening PR: `pre-commit run -a`, run mail CLI/dbt commands you touched, update docs (e.g., `services/mail/README.md`).

## Security & Configuration
- Never commit secrets. Store credentials in `.env` and `gcs_key.json` (gitignored).
- Set `GOOGLE_APPLICATION_CREDENTIALS` to the path of `gcs_key.json` for BigQuery/dbt.
