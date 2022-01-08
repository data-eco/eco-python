"""
Io Packager class definition
"""
import datetime
import frictionless
import os
import pathlib
import sys
import uuid
import yaml
from iodat.summary import compute_summary
from iodat.version import __version__
from iodat.util import load_tbl
from cerberus import Validator
from typing import Any, Dict
from pkg_resources import resource_filename

class Packager:
    def __init__(self):
        """
        Creates a new Packager instance
        """
        self._schema_dir = os.path.abspath(resource_filename(__name__, "schema"))

        self._version = "0.6.0"

    def _load_recipe(self, recipe: str | pathlib.Path | Dict[str, Any]) -> Dict[str, Any]:
        """Loads  & validates io recipe"""
        # if path specified, attempt to load recipe from file
        #if type(recipe) is str or type(recipe) is pathlib.Path:
        #if isinstance(recipe, str) or isinstance(recipe, pathlib.Path):
        if isinstance(recipe, (str, pathlib.Path,)):
            if not os.path.exists(recipe):
                raise FileNotFoundError("Unable to find recipe at specified location!")

            with open(recipe) as fp:
                recipe = yaml.load(fp, Loader=yaml.FullLoader)

        assert isinstance(recipe, dict)

        # load recipe schema & validate
        with open(os.path.join(self._schema_dir, "recipe.yml")) as schema_fp:
            schema = yaml.load(schema_fp, Loader=yaml.FullLoader)

        # include profile schema, if specified
        if "profile" in recipe:
            if recipe['profile'] not in schema['profile']['allowed']:
                raise Exception("Unrecognized profile specified!")

            if recipe['profile'] != "general":
                infile = os.path.join(self._schema_dir, "profiles", f"{recipe['profile']}.yml")

                with open(infile) as profile_fp:
                    profile = yaml.load(profile_fp, Loader=yaml.FullLoader)

                    schema[recipe['profile']] = {
                        "type": 'dict',
                        "required": True,
                        "schema": profile
                    }

        validator = Validator(schema)

        if validator.validate(recipe, schema) is not True:
            print(f"Recipe validation failed!")
            print(validator.errors)
            sys.exit(1)

        return recipe

    #  def build_package(self, recipe, pkg_dir, include_summary=False):
    def build_package(self, recipe: str | pathlib.Path | Dict[str, Any], resources, pkg_dir="./", include_summary=False):
        """Creates a datapackage.yml file for a given dataset"""
        # load & validate recipe
        self.recipe = self._load_recipe(recipe)

        # cd to the directory containing the package data;
        # at present, it's not possible to parse files outside of direct/child directories
        #os.chdir(pkg_dir)

        pkg_dir = os.path.realpath(os.path.expanduser(os.path.expandvars(pkg_dir)))

        # create a new DataPackage instance and set relevant fields
        # TEMP Jan 5, 2022: until i can figure out how to instantiate Packages/Resources
        # from dataframes directly, temporarily just write them to disk and read it back
        # in...
        #pkg = frictionless.describe_package("*.tsv", basepath=pkg_dir)
        for resource_name in resources:
            outfile = os.path.join(pkg_dir, resource_name + ".csv")
            resources[resource_name].to_csv(outfile)
        pkg = frictionless.describe_package("*.csv", basepath=pkg_dir)

        # add num rows/cols to resource descriptions
        for i, resource in enumerate(pkg["resources"]):
            #fname = os.path.basename(resource["path"])

            # add data dimensions
            # TODO: make optional and/or handle along-side of compute_stats to avoid
            # having to read files multiple times..
            #dat = load_tbl(fname)

            pkg["resources"][i]["num_rows"] = resources[resource['name']].shape[0]
            pkg["resources"][i]["num_columns"] = resources[resource['name']].shape[1]

        now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        mdata = self.recipe

        pkg_uuid = str(uuid.uuid4())
        mdata['data']['uuid'] = pkg_uuid 

        mdata['provenance'] = {
            "nodes": {
                pkg_uuid: mdata['provenance']
            },
            "edges": {}
        }

        # add packager-related details
        mdata['provenance']['nodes'][pkg_uuid]['software'] = {
            "name": "io-python",
            "time":  now,
            "version": self._version
        }

        # add io metadata
        pkg["io"] = mdata

        # add summary statistics
        # todo: allow summary stats to be enabled at the resource-level?
        #  if include_summary:
        #      try:
        #          data_path = os.path.join(pkg_dir, "data.tsv")
        #          pkg["summary"] = compute_summary(data_path)
        #      except:
        #          raise RuntimeError("Error computing dataset summary")

        return pkg
