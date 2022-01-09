io-python
=========

**Status (Jan 2022)**: Planning / experimental.

Overview
--------

_io-dag ~ metadata for data workflows_

`io-python` is a python library for creating and interacting with [Frictionless Data
Packages](https://frictionlessdata.io/) which include a Directed Acyclic Graph (DAG)
representation of data provenance, along with optional annotations and views for each
stage in processing.

The goal is to provide a means by which users can transparently document all of the
reasoning, assumptions, insights and questions that are typically involved in any data
analysis, in a format which can be easily stored in version control and iteratively
improved upon by the community of users who care about the data and would like to gain a
deeper understanding of it.

The provenance DAG, along with associated annotations and visualizations, can then be
rendered in various endpoints such as a CLI tool or Web UI, with additional information
about the data at each stage of processing (shape, % missing or imputed values, etc.)
can be overlaid on the DAG, providing useful visual context to data users.

With all of this, a major goal is to try and convey _uncertainty_ as transparently as
possible.

_The Basic Idea_:

1. Users modify their pipeline to include io "hooks" to generate & update datapackages
   for each stage of processing.
2. Data packages with embedded DAGs written out at each stage of processing
  - Julia/Python/R libraries help with datapackage i/o & DAG operations
3. For each stage in data processing (node in the DAG), one can optionally include one
   or more, each of:
   - annotations (markdown, or any other plain-text format)
   - data plots ([vega-lite](https://vega.github.io/))

The resulting output is a collection of _data packages_, each of which has a complete
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

Current efforts are focused at supporting "tabular" (matrix/dataframe) data packages.
Long-term, the goal is is to extend support to other forms of data.

TODO
----

- [ ] extend support to alt file formats
  - [ ] tsv
  - [ ] feather/arrow
- [ ] allow json to be passed in for metadata
- [ ] describe motivation & use of profiles
- [ ] allow arbitrary schemas to be provided & used for validation?
  - [ ] modify "profile" to accept a path/url/dict?
