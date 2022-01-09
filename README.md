Io Python
=========

**Status (Jan 2022)**: Planning / experimental.

Overview
--------

Tools for creating and interacting with [Frictionless Data
Packages](https://frictionlessdata.io/) which including a Directed Acyclic Graph (DAG)
representation of data provenance, along with optional annotations and views for each
stage in processing.

_The Basic Idea_:

1. Sits on top of existing pipeline codebase
2. Data packages with embedded DAGs written out at each stage of processing
  - Julia/Python/R libraries help with datapackage i/o & DAG operations
3. For each stage in data processing (node in the DAG), one can optionally include one
   or more, each of:
   - annotations (markdown, or any other plain-text format)
   - data plots ([vega-lite](https://vega.github.io/))

The result is a collection of data packages as output, each of which has a complete
history of all of the operations performed on it up to the point, including annotations
and visualizations for each stage, intended to help convey the reasoning, motivation,
uncertainaties, etc. associated with the processing steps.

Components
----------

1. Libraries (R/Python/Julia)
  - read/write DataDAGs
2. UIs
  - CLI
  - Web (Planned)

Goals
-----

1. Data/code agnostic
2. Progressive
  - basic DAG functionality for any datapackage
  - additional metadata fields used for visualization, etc., if provided
  
Limitations
-----------

1. Currently focused on supporting tabular data packages
  - long-term, if useful, would like to extend to other forms of data

TODO
----

- [ ] extend support to alt file formats
  - [ ] tsv
  - [ ] feather/arrow
- [ ] allow json to be passed in for metadata
- [ ] describe motivation & use of profiles
- [ ] allow arbitrary schemas to be provided & used for validation?
  - [ ] modify "profile" to accept a path/url/dict?


