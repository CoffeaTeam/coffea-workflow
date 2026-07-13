# Optimisation

*(Coming soon)*

The goal of this section is to measure and compare the performance of the AGC ttbar analysis across different configurations:

- **Facilities**: local, CoffeaCasa, lxplus
- **Executors**: `FuturesExecutor`, `DaskExecutor`
- **Split strategies**: no split, by dataset, by dataset + percentage
- **Parallelism modes**: sequential chunk processing vs `client.submit` for parallel chunk dispatch on CoffeaCasa

Metrics to collect: wall-clock time, throughput (events/s), cache hit rate, failure recovery time?

## Sequential vs Parallel submission to Dask
Let's assume:
We have N workers, M chunks (subsets), K event-range tasks per chunk:


Sequential:  M × (K/N) × T    — M rounds, each using all N workers on K tasks
Parallel:    ceil(M/N) × K × T — ceil(M/N) rounds, each worker does K tasks alone


1. Idle time between chunks (sequential only)


Sequential:
while notebook merges + resubmits

Parallel:
no idle gaps, just doing its chunk

2. Load imbalance

Sequential: if chunk_A has 30 tasks and chunk_B has 6 tasks,
  all N workers finish chunk_A, then all N workers finish chunk_B quickly
  → workers self-balance across tasks within each chunk

Parallel: W1 gets chunk_A (30 tasks), W2 gets chunk_B (6 tasks)
  W2 finishes 5× faster and sits idle while W1 is still going
  → the slowest chunk sets the total time
  
3. When N > M
N=10 workers, M=5 chunks, K=12 tasks/chunk:

Sequential: 5 × (12/10) × T = 6T    ← uses all 10 workers per chunk


Parallel:   12T                       ← 5 workers idle the whole time, each
                                         worker does 12 tasks alone

Sequential wins here.


4. When N < M

N=5 workers, M=20 chunks, K=12 tasks/chunk:

Sequential: 20 × (12/5) × T = 48T

Parallel:   4 rounds × 12T = 48T     ← same, but fewer submission round-trips



### Condition	Better choice

- N > M (more workers than chunks)	Sequential — all workers collaborate per chunk
- N ≈ M	Equal compute; parallel wins slightly on overhead
- N < M	Equal compute; parallel wins on overhead and idle time
- Chunks have unequal file counts	Sequential — workers self-balance across tasks
- Chunks are equal in size	Parallel is fine, no imbalance issue

