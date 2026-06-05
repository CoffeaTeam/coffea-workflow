# Optimisation

*(Coming soon)*

The goal of this section is to measure and compare the performance of the AGC ttbar analysis across different configurations:

- **Facilities**: local, CoffeaCasa, lxplus
- **Executors**: `FuturesExecutor`, `DaskExecutor`
- **Split strategies**: no split, by dataset, by dataset + percentage
- **Parallelism modes**: sequential chunk processing vs `client.submit` for parallel chunk dispatch on CoffeaCasa vs
chunk-level parallelism via Snakemake

Metrics to collect: wall-clock time, throughput (events/s), cache hit rate, failure recovery time.
