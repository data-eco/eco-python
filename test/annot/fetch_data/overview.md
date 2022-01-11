Penguin Data Retrieval
======================

For this example, the [palmerpenguins](https://github.com/allisonhorst/palmerpenguins)
dataset is downloaded using [seaborn](http://seaborn.pydata.org/).

For the first data package generated in a workflow, the corresponding DAG will be a
single nodes with no edges.

The `DAG.add_node()` function accepts optional parameters "annot" and "views".

Each expects a _list_ containing one or more markdown annotations or vega-lite views,
respectively.

If specified, they will be parse and added to the resulting datapackage.json.
