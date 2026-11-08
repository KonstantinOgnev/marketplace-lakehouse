"""Pure configuration helpers that can be tested without a Spark session."""

from dataclasses import dataclass

VALID_ENVIRONMENTS = {"dev", "test", "prod"}
VALID_MODES = {"incremental", "backfill", "replay", "full-refresh"}


@dataclass(frozen=True)
class PipelineConfig:
    environment: str
    catalog_name: str
    mode: str = "incremental"

    def validate(self) -> "PipelineConfig":
        if self.environment not in VALID_ENVIRONMENTS:
            raise ValueError(f"Unsupported environment: {self.environment}")
        if self.mode not in VALID_MODES:
            raise ValueError(f"Unsupported mode: {self.mode}")
        if not self.catalog_name or "." in self.catalog_name:
            raise ValueError("catalog_name must be a non-empty single identifier")
        return self


def expected_catalog(environment: str) -> str:
    if environment == "prod":
        return "marketplace_prod"
    if environment in {"dev", "test"}:
        return "marketplace_lab"
    raise ValueError(f"Unsupported environment: {environment}")
