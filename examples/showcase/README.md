# coffea-workflow showcase

A minimal benchmark MET analysis on CMS NanoAOD open data, used to demonstrate the capabilities of the `coffea-workflow` package.

## Analysis

[analysis.py](analysis.py) contains a self-contained coffea processor that fills a MET histogram over two datasets (`SingleMu_0`, `SingleMu_1`), each with 5 NanoAOD files from CMS Open Data. 
It is intentionally primitive - the point is to show how `coffea-workflow` wraps any analysis, not to do physics. One of the files is also intentionally broken
to showcase how coffea-workflow helps to preserve partial results and to run the analysis until the end.

## Showcase directories

| Directory | What it shows |
|---|---|
| [split_strategy/](split_strategy/) | How to split the fileset into chunks and the trade-offs of each approach |
| [coffea_casa/](coffea_casa/) | Running with `CoffeaCasaFactory` and a Dask cluster |
| [lxplus/](lxplus/) | Building an Apptainer image and running with `LxplusFactory` on CERN lxplus |
| [optimisation/](optimisation/) | *(under development)* Performance measurements across facilities, executors, and split strategies |
