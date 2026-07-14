# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-09

Initial release.

### Added

- **Artifact-based workflow engine**: analyses are declared as `Step`s in a
  `Workflow` DAG; each stage is a typed, hashable artifact (`Fileset`,
  `Analysis`, `Plotting`, `CustomArtifact`) with a deterministic identity
  derived from its inputs.
- **Content-addressable caching**: every produced artifact is stored under
  `.cache/`; steps whose inputs have not changed are loaded from disk on the
  next run.
- **Chunk-level fault tolerance**: failed chunks (e.g. a broken XRootD
  endpoint) are recorded via the coffea `Ok`/`Err` result-type protocol
  (`processor.Runner(use_result_type=True)`); successful chunks are preserved
  and only failed ones are retried on the next run.
- **Configurable chunk dispatch**: splitting strategies (`by_dataset`, ...),
  `percentage` subsetting, and parallel chunk submission to Dask workers
  (`parallel_chunks=True`).
- **Facility factories** for common HEP analysis facilities: local execution,
  coffea-casa, and lxplus/HTCondor, including an Apptainer worker-image
  definition generator (`generate_apptainer_def`).
- **histserv integration**: server-side histogram filling via
  `RunConfig(hist_client=..., histserv_connection_info=...)`.

### Requirements

- Python >= 3.10
- coffea >= 2026.7.0 (first release exposing `use_result_type` on
  `processor.Runner`)

[0.1.0]: https://github.com/CoffeaTeam/coffea-workflow/releases/tag/v0.1.0
