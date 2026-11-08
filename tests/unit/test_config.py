import pytest

from marketplace_lab.config import PipelineConfig, expected_catalog


def test_valid_dev_configuration():
    config = PipelineConfig("dev", "marketplace_lab", "incremental")
    assert config.validate() is config


def test_prod_catalog_mapping():
    assert expected_catalog("prod") == "marketplace_prod"


@pytest.mark.parametrize("mode", ["incremental", "backfill", "replay", "full-refresh"])
def test_supported_modes(mode):
    PipelineConfig("dev", "marketplace_lab", mode).validate()


def test_rejects_unknown_environment():
    with pytest.raises(ValueError, match="Unsupported environment"):
        PipelineConfig("demo", "marketplace_lab").validate()


def test_rejects_qualified_catalog_name():
    with pytest.raises(ValueError, match="single identifier"):
        PipelineConfig("dev", "marketplace_lab.gold").validate()
