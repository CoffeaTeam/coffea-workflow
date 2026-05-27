"""
Tests for render._resolve_step_config.
"""
import pytest
from coffea_workflow.config import RunConfig, ExecutorConfig, FacilityConfig
from coffea_workflow.workflow import Step
from coffea_workflow.artifacts import Fileset
from coffea_workflow.render import _resolve_step_config


@pytest.fixture
def workflow_config():
    return RunConfig(
        strategy="by_dataset",
        percentage=50,
        cache_dir=".cache",
        executor_config=ExecutorConfig(executor_type="FuturesExecutor", workers=2),
        facility=FacilityConfig(name="local"),
    )


@pytest.fixture
def bare_step():
    return Step(name="S", step_type=Fileset, builder="m:fn")


class TestResolveStepConfig:
    def test_no_overrides_returns_workflow_config(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result is workflow_config

    def test_step_facility_overrides_workflow_facility(self, workflow_config):
        step = Step(
            name="S", step_type=Fileset, builder="m:fn",
            facility=FacilityConfig(name="coffea-casa", scheduler_address="tcp://x:8786"),
        )
        result = _resolve_step_config(workflow_config, step)
        assert result.facility.name == "coffea-casa"

    def test_step_facility_does_not_affect_analysis_params(self, workflow_config):
        step = Step(
            name="S", step_type=Fileset, builder="m:fn",
            facility=FacilityConfig(name="coffea-casa", scheduler_address="tcp://x:8786"),
        )
        result = _resolve_step_config(workflow_config, step)
        assert result.strategy == workflow_config.strategy
        assert result.percentage == workflow_config.percentage
        assert result.cache_dir == workflow_config.cache_dir

    def test_step_executor_config_overrides_workflow_executor(self, workflow_config):
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert result.executor_config.executor_type == "IterativeExecutor"

    def test_step_executor_does_not_affect_analysis_params(self, workflow_config):
        ec = ExecutorConfig(executor_type="IterativeExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert result.strategy == workflow_config.strategy
        assert result.cache_dir == workflow_config.cache_dir

    def test_both_overrides_applied_independently(self, workflow_config):
        fc = FacilityConfig(name="coffea-casa", scheduler_address="tcp://x:8786")
        ec = ExecutorConfig(executor_type="DaskExecutor")
        step = Step(name="S", step_type=Fileset, builder="m:fn", facility=fc, executor_config=ec)
        result = _resolve_step_config(workflow_config, step)
        assert result.facility.name == "coffea-casa"
        assert result.executor_config.executor_type == "DaskExecutor"
        assert result.strategy == workflow_config.strategy

    def test_workflow_facility_used_when_step_has_none(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result.facility is workflow_config.facility

    def test_workflow_executor_used_when_step_has_none(self, workflow_config, bare_step):
        result = _resolve_step_config(workflow_config, bare_step)
        assert result.executor_config is workflow_config.executor_config
