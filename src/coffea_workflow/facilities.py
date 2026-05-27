"""
Pre-built FacilityConfig instances for common HEP computing facilities.

Usage:
    import workflow.facilities as facilities

    # whole workflow on one facility
    config = RunConfig(facility=facilities.coffea_casa, ...)

    # per-step override
    step = Step(..., facility=facilities.local)

Scheduler addresses are resolved from environment variables at execution time:
    coffea_casa  ->  COFFEA_CASA_SCHEDULER
    lxplus       ->  LXPLUS_DASK_SCHEDULER

Override with an explicit address:
    FacilityConfig(name="coffea-casa", scheduler_address="tcp://my-host:8786")
"""

from .config import FacilityConfig

local = FacilityConfig(name="local")
coffea_casa = FacilityConfig(name="coffea-casa")
lxplus = FacilityConfig(name="lxplus")