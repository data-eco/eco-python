"""
eco CLI
"""
import os
import yaml
from argparse import ArgumentParser
from rich import print


class CLI:
    """Eco Command-line Interface class"""

    def __init__(self):
        """Initializes a new CLI instance"""
        self._get_args()
        self._load_pkg()
        self._print_header()

        # TODO: think about pre-generating and/or caching visualizations so that
        # they can be rendered directly... need to have some conventions..
        if self._args["action"] == "viz":
            # load libraries needed for viz
            import tempfile
            import seaborn as sns
            from pixcat import Image

            sns.set_theme()

            # load data
            self._load_dataset()

            # build heatmap (TESTING)
            ax = sns.heatmap(self._dat.iloc[:10, :10])

            # store to temporary file and render in terminal
            outfile = tempfile.mktemp() + ".png"
            figure = ax.get_figure()

            figure.savefig(outfile, dpi=300)
            Image(outfile).show()

        elif self._args["action"] == "row":
            import tempfile
            import pandas as pd
            import seaborn as sns
            from pixcat import Image

            self._load_dataset()

            # get row values
            # TODO: check to make sure single additional param provided, and that
            # it is a value present in the index..
            row_id = self._args["params"][0]
            vals = self._dat[self._dat.index == row_id]

            # get pre-computed summary stats
            mdat = self._pkg["summary"]["rows"]

            # compute stats
            stats = {
                "mean": float(vals.mean(axis=1)),
                "median": float(vals.median(axis=1)),
                "min": float(vals.min(axis=1)),
                "max": float(vals.max(axis=1)),
            }

            # TODO.. just show global stats instead? this is kinda hard to interpret..
            # average stats for _all_ rows
            mean_mean = mdat["mean_mean"]
            med_mean = mdat["median_mean"]

            heading = "-" * len(row_id)

            print(f"[cyan]{heading}[/cyan]")
            print(f"[bold yellow]{row_id}[/bold yellow]")
            print(f"[cyan]{heading}[/cyan]")
            print("")
            print("Row statistics (mean for all rows shown in parens):")
            print("")
            print(f"[magenta]Mean:[/magenta] {stats['mean']:.2f} ({mean_mean:.2f})")
            print(f"[magenta]Median:[/magenta] {stats['median']:.2f} ({med_mean:.2f})")
            print(f"[magenta]Min:[/magenta] {stats['min']:.2f}")
            print(f"[magenta]Max:[/magenta] {stats['max']:.2f}")
            print("")

            # TODO: quantiles/ranks (i.e. how do above vals compare to other rows in the
            # dataset?.. may want to read from datapackage.yml...)

            # density plot
            long_df = pd.melt(vals)
            ax = sns.displot(long_df, x=long_df.value, kind="kde")
            ax.set(xlabel=row_id)

            # store to temporary file and render in terminal
            outfile = tempfile.mktemp() + ".png"
            figure = ax.fig

            figure.savefig(outfile, dpi=196)
            Image(outfile).show()

    def _load_dataset(self):
        import pandas as pd

        #  import numpy as np
        #  np.random.seed(0)

        # resource metadata
        for resource in self._pkg["resources"]:
            if resource["type"] == "dataset":
                res = resource

        # load dataset
        dat_path = os.path.join(os.path.dirname(self._args["target"]), res["path"])

        self._dat = pd.read_csv(dat_path, sep="\t")
        self._dat = self._dat.set_index(self._dat.columns[0])

    def _load_pkg(self):
        """Loads eco package yaml"""
        # check to make sure a valid
        pkg_path = self._args["target"]

        if not os.path.exists(pkg_path) or not os.path.isfile(pkg_path):
            raise FileNotFoundError("Unable to find datapackage.yml: " + pkg_path)

        with open(pkg_path) as fp:
            pkg = yaml.load(fp, Loader=yaml.FullLoader)

        self._pkg = pkg

    def _get_args(self):
        """Parses input and returns arguments"""
        parser = ArgumentParser(description="Eco Command-line Data Explorer")

        parser.add_argument(
            "action",
            help="Action to perform (default: summary)",
            nargs="?",
            default="summary",
        )

        parser.add_argument(
            "-t",
            "--target",
            help="Path to target eco datapackage directory",
            default=os.getcwd(),
        )

        parser.add_argument(
            "params",
            metavar="PARAMS",
            type=str,
            nargs="*",
            help="Additional arguments for action.",
        )

        # convert command-line args to a dict and return
        args = parser.parse_args()

        args = dict((k, v) for k, v in list(vars(args).items()) if v is not None)

        # replace target directory with a datapackage path
        if os.path.isdir(args["target"]):
            args["target"] = os.path.join(args["target"], "datapackage.yml")

        self._args = args

    def _print_header(self):
        """Prints output header"""
        # eco metadata
        mdat = self._pkg["eco"]

        # resource metadata
        for resource in self._pkg["resources"]:
            if resource["type"] == "dataset":
                res = resource

        # note: using a work-around to determine number of columns;
        # packaging step should be modified to include simple row/col counts elsewhere..
        # num_rows = ...
        num_cols = len(res["schema"]["fields"])

        print("[cyan]========================================[/cyan]")
        print(":microscope:", "[bold green]eco explorer[/bold green]")
        print("[cyan]========================================[/cyan]")
        print(f"[cyan]Title[/cyan]: {mdat['data']['dataset']['title']}")
        print(f"[cyan]Source[/cyan]: {mdat['data']['datasource']['title']}")
        print(f"[cyan]Experiment[/cyan]: {mdat['data']['experiment']['title']}")
        print(f"[cyan]Processing[/cyan]: {mdat['data']['processing']}")
        # print(f"[magenta]# Rows:[/magenta] {num_rows}")
        print(f"[magenta]# Cols:[/magenta] {num_cols}")
        print("")
