Data-DAG Example Pipeline
=========================

**Setup**:

Use [conda](https://docs.conda.io/en/latest/) to create a new environment and install
the necessary components:

```
conda create -n io-snakemake-demo --file requirements.txt
```

**Usage**:

Activate the conda environment using:

```
conda activate io-snakemake-demo
```

Next, to run the pipeline, call:

```
snakemake -j1
```

The `-j1` flag indicates that a single thread should be used.

**Data**:

The demo makes use of the
[palmerpenguins](https://github.com/allisonhorst/palmerpenguins) dataset from 
[Gorman et al. (2014)](https://doi.org/10.1371/journal.pone.0090081).

