"""
DataPackage class
"""
import os
import json
import yaml
import pandas as pd
import altair as alt
from altair.utils.data import to_values

class DataPackage:
    def __init__(self, pkg_path):
        """Creates a new DataPackage instance"""
        with open(pkg_path) as fp:
            if pkg_path.endswith("json"):
                pkg = json.load(fp)
            elif pkg_path.endswith("yml") or pkg_path.endswith("yaml"):
                pkg = yaml.load(fp, Loader=yaml.FullLoader)
            else:
                raise Exception(f"Unexpected file extension for datapackage \"{pkg_path}\"")

        self._pkg = pkg
        self._pkg_dir = str(os.path.dirname(pkg_path))

        self.uuid = pkg['eco']['uuid']

        self.dag = {
            "nodes": pkg['eco']['nodes'],
            "edges": pkg['eco']['edges'],
        }

        self.dataset = pkg['eco']['metadata']['data']['dataset']
        self.source = pkg['eco']['metadata']['data']['source']
        self.version = pkg['eco']['metadata']['data']['version']

        self.rows = pkg['eco']['metadata']['rows']
        self.columns = pkg['eco']['metadata']['columns']
        self.provenance = pkg['eco']['metadata']['provenance']

        # check for known profiles
        self.profile = None

        if "biodat" in pkg['eco']['metadata']:
            self.profile = 'biodat'
            self.biodat = pkg['eco']['metadata']['biodat']

        # set initial focus in DAG to node representing current data stage
        self.focus = self.uuid

        # store view data, as needed
        self.views = {}

    def get_annots(self, node_uuid=None):
        if node_uuid == None:
            node_uuid = self.focus

        return self.dag['nodes'][node_uuid]['annot']

    def get_resource(self, ind=0):
        filename = self._pkg['resources'][ind]['path']
        infile = os.path.join(self._pkg_dir, filename)
        return pd.read_csv(infile)

    def get_resource_info(self, ind=0):
        return self._pkg['resources'][ind]

    def get_views(self, node_uuid=None):
        if node_uuid == None:
            node_uuid = self.focus

        return self.dag['nodes'][node_uuid]['views']

    def get_view_names(self, node_uuid=None):
        return [view['name'] for view in self.get_views(node_uuid)]
        
    def plot(self, node_uuid=None, view_name=None):
        if node_uuid == None:
            node_uuid = self.focus

        # determine name of view to show
        view_ind = None

        if view_name is None:
            view_ind = 0
            view_name = self.dag['nodes'][node_uuid]['views'][view_ind]['name']
        else:
            for i, view in enumerate(self.dag['nodes'][node_uuid]['views']):
                if view['name'] == view_name:
                    view_ind = i

        if view_ind == None:
            raise Exception("Unrecognized view name specified!")

        # check to see if view has previously been generated, and if so, reuse
        plot_dir = os.path.join(self._pkg_dir, "plot")

        if not os.path.exists(plot_dir):
            os.makedirs(plot_dir, mode=0o755)

        plot_path = os.path.join(plot_dir, f"{view_name}.png")

        if not os.path.exists(plot_path):
            # load spec
            spec = self.dag['nodes'][node_uuid]['views'][view_ind].copy()

            # load data

            if view_name not in self.views:
                data_path = os.path.join(self._pkg_dir, spec['data'] + '.csv')
                self.views[view_name] = to_values(pd.read_csv(data_path))

            spec['data'] = self.views[view_name]

            chart = alt.Chart.from_dict(spec)
            chart.save(plot_path)

        try:
            from pixcat import Image
            Image(plot_path).show()
        except:
            print(f"Plot saved to {plot_path}")

