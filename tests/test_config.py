"""
Tests for coffea_workflow/config.py
 
RunConfig is a frozen dataclass. __post_init__ validates:
  - strategy must be None or "by_dataset"
  - percentage (when set) must be an int, 1-100, and divide 100 evenly
  - datasets list is auto-converted to tuple for hashability
  - chunk_fraction (when set) must be a float in (0.0, 1.0]
"""
import pytest
from pathlib import Path
from coffea_workflow.config import RunConfig, ExecutorConfig, FacilityBase
from coffea_workflow.facilities import LocalFactory, CoffeaCasaFactory
 
 
class TestRunConfigDefaults:
    def test_strategy_is_none(self):
        assert RunConfig().strategy is None
 
    def test_percentage_is_none(self):
        assert RunConfig().percentage is None
 
    def test_datasets_is_none(self):
        assert RunConfig().datasets is None
 
    def test_chunk_fraction_is_none(self):
        assert RunConfig().chunk_fraction is None
 
    def test_cache_dir_default(self):
        assert RunConfig().cache_dir == Path(".cache")
 
    def test_hist_client_is_none(self):
        assert RunConfig().hist_client is None
 
    def test_histserv_connection_info_is_none(self):
        assert RunConfig().histserv_connection_info is None
 
 
class TestRunConfigStrategy:
    def test_none_is_valid(self):
        assert RunConfig(strategy=None).strategy is None
 
    def test_by_dataset_is_valid(self):
        assert RunConfig(strategy="by_dataset").strategy == "by_dataset"
 
    def test_unknown_strategy_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid strategy"):
            RunConfig(strategy="by_file")
 
 
class TestRunConfigPercentage:
    @pytest.mark.parametrize("pct", [1, 2, 4, 5, 10, 20, 25, 50, 100])
    def test_valid_percentages(self, pct):
        cfg = RunConfig(percentage=pct)
        assert cfg.percentage == pct
 
    @pytest.mark.parametrize("pct", [3, 30, 40, 70, 99])
    def test_non_divisor_raises_value_error(self, pct):
        with pytest.raises(ValueError, match="percentage must divide 100 evenly"):
            RunConfig(percentage=pct)
 
    def test_zero_raises_value_error(self):
        with pytest.raises(ValueError, match="percentage must be between"):
            RunConfig(percentage=0)
 
    def test_negative_raises_value_error(self):
        with pytest.raises(ValueError, match="percentage must be between"):
            RunConfig(percentage=-10)
 
    def test_over_100_raises_value_error(self):
        with pytest.raises(ValueError, match="percentage must be between"):
            RunConfig(percentage=101)
 
    def test_float_raises_type_error(self):
        with pytest.raises(TypeError, match="percentage must be an int"):
            RunConfig(percentage=20.0)
 
    def test_string_raises_type_error(self):
        with pytest.raises(TypeError, match="percentage must be an int"):
            RunConfig(percentage="20")
 
 
class TestRunConfigDatasets:
    def test_list_is_converted_to_tuple(self):
        cfg = RunConfig(datasets=["A", "B", "C"])
        assert isinstance(cfg.datasets, tuple)
        assert cfg.datasets == ("A", "B", "C")
 
    def test_tuple_is_unchanged(self):
        cfg = RunConfig(datasets=("X", "Y"))
        assert cfg.datasets == ("X", "Y")
 
    def test_none_stays_none(self):
        assert RunConfig(datasets=None).datasets is None
 
 
class TestRunConfigChunkFraction:
    def test_valid_half(self):
        assert RunConfig(chunk_fraction=0.5).chunk_fraction == 0.5
 
    def test_valid_one(self):
        assert RunConfig(chunk_fraction=1.0).chunk_fraction == 1.0
 
    def test_zero_raises(self):
        with pytest.raises(ValueError, match="chunk_fraction"):
            RunConfig(chunk_fraction=0.0)
 
    def test_int_one_raises(self):
        # must be float, not int
        with pytest.raises(ValueError, match="chunk_fraction"):
            RunConfig(chunk_fraction=1)
 
    def test_over_one_raises(self):
        with pytest.raises(ValueError, match="chunk_fraction"):
            RunConfig(chunk_fraction=1.5)
 
    def test_negative_raises(self):
        with pytest.raises(ValueError, match="chunk_fraction"):
            RunConfig(chunk_fraction=-0.5)
 
 
class TestRunConfigFrozen:
    def test_cannot_mutate_strategy(self):
        cfg = RunConfig()
        with pytest.raises(Exception):
            cfg.strategy = "by_dataset"
 
    def test_cannot_mutate_cache_dir(self):
        cfg = RunConfig()
        with pytest.raises(Exception):
            cfg.cache_dir = Path("/new/path")


# ---------------------------------------------------------------------------
# ExecutorConfig
# ---------------------------------------------------------------------------

class TestExecutorConfigDefaults:
    def test_executor_type_default(self):
        assert ExecutorConfig().executor_type == "FuturesExecutor"

    def test_workers_default(self):
        assert ExecutorConfig().workers == 1

    def test_chunks_per_worker_default(self):
        assert ExecutorConfig().chunks_per_worker == 1

    def test_dask_scheduler_default_is_none(self):
        assert ExecutorConfig().dask_scheduler is None

    def test_executor_override_default_is_none(self):
        assert ExecutorConfig().executor is None


class TestExecutorConfigValidation:
    def test_invalid_executor_type_raises(self):
        with pytest.raises(ValueError, match="Invalid executor_type"):
            ExecutorConfig(executor_type="spark")

    def test_dask_without_scheduler_is_valid(self):
        # scheduler address is no longer required at construction —
        # it can come from a FacilityConfig at build_executor() time
        ec = ExecutorConfig(executor_type="DaskExecutor")
        assert ec.dask_scheduler is None

    def test_dask_with_scheduler_ok(self):
        ec = ExecutorConfig(executor_type="DaskExecutor", dask_scheduler="tcp://host:8786")
        assert ec.dask_scheduler == "tcp://host:8786"

    def test_workers_zero_raises(self):
        with pytest.raises(ValueError, match="workers"):
            ExecutorConfig(workers=0)

    def test_workers_negative_raises(self):
        with pytest.raises(ValueError, match="workers"):
            ExecutorConfig(workers=-1)

    def test_chunks_per_worker_zero_raises(self):
        with pytest.raises(ValueError, match="chunks_per_worker"):
            ExecutorConfig(chunks_per_worker=0)

    def test_raw_executor_skips_all_validation(self):
        from unittest.mock import MagicMock
        fake = MagicMock()
        # executor_type would normally fail, but is ignored when executor is set
        ec = ExecutorConfig(executor_type="spark", workers=-99, executor=fake)
        assert ec.executor is fake


class TestRunConfigExecutorConfig:
    def test_default_executor_config_is_none(self):
        assert RunConfig().executor_config is None

    def test_stores_executor_config(self):
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        cfg = RunConfig(executor_config=ec)
        assert cfg.executor_config is ec

    def test_frozen_executor_config_field(self):
        cfg = RunConfig()
        with pytest.raises(Exception):
            cfg.executor_config = ExecutorConfig()


# ---------------------------------------------------------------------------
# LocalFactory
# ---------------------------------------------------------------------------

class TestLocalFactory:
    def test_default_workers(self):
        assert LocalFactory().workers == 4

    def test_default_scheduler_address_is_none(self):
        assert LocalFactory().scheduler_address is None

    def test_custom_workers(self):
        assert LocalFactory(workers=8).workers == 8

    def test_is_facility_base(self):
        assert isinstance(LocalFactory(), FacilityBase)

    def test_dask_without_address_raises(self):
        ec = ExecutorConfig(executor_type="DaskExecutor", dask_scheduler=None)
        with pytest.raises(ValueError, match="scheduler address"):
            LocalFactory().build(ec)

    def test_dask_with_scheduler_on_ec(self, monkeypatch):
        pytest.importorskip("distributed")
        monkeypatch.setattr("dask.distributed.Client", lambda addr: object())
        ec = ExecutorConfig(executor_type="DaskExecutor", dask_scheduler="tcp://host:8786")
        LocalFactory().build(ec)  # should not raise


# ---------------------------------------------------------------------------
# CoffeaCasaFactory
# ---------------------------------------------------------------------------

class TestCoffeaCasaFactory:
    def test_default_scheduler_address(self):
        assert CoffeaCasaFactory().scheduler_address == "tls://localhost:8786"

    def test_custom_scheduler_address(self):
        f = CoffeaCasaFactory(scheduler_address="tls://myhost:8786")
        assert f.scheduler_address == "tls://myhost:8786"

    def test_worker_packages_coerced_to_tuple(self):
        f = CoffeaCasaFactory(worker_packages=["pkg1", "pkg2"])
        assert f.worker_packages == ("pkg1", "pkg2")

    def test_worker_files_coerced_to_tuple(self):
        f = CoffeaCasaFactory(worker_files=["a.py"])
        assert f.worker_files == ("a.py",)

    def test_is_facility_base(self):
        assert isinstance(CoffeaCasaFactory(), FacilityBase)

    def test_raw_executor_returned_directly(self):
        from unittest.mock import MagicMock
        fake = MagicMock()
        ec = ExecutorConfig(executor_type="spark", workers=-1, executor=fake)
        assert CoffeaCasaFactory().build(ec) is fake


# ---------------------------------------------------------------------------
# RunConfig.facility
# ---------------------------------------------------------------------------

class TestRunConfigFacility:
    def test_facility_default_is_none(self):
        assert RunConfig().facility is None

    def test_stores_local_factory(self):
        f = LocalFactory()
        cfg = RunConfig(facility=f)
        assert cfg.facility is f

    def test_stores_coffea_casa_factory(self):
        f = CoffeaCasaFactory()
        cfg = RunConfig(facility=f)
        assert cfg.facility is f

    def test_frozen_facility_field(self):
        cfg = RunConfig()
        with pytest.raises(Exception):
            cfg.facility = LocalFactory()