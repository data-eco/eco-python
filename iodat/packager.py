"""
Io Packager class definition
"""
import datetime
import frictionless
import json
import os
import pathlib
import sys
import uuid
import yaml
from iodat.summary import compute_summary
from iodat.version import __version__
from iodat.util import load_tbl
from cerberus import Validator
from typing import Any, Dict, List
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

    def build_package(self, recipe: str | pathlib.Path | Dict[str, Any], resources, pkg_dir="./", 
                      annotations=List[str | pathlib.Path], 
                      views=List[str | pathlib.Path | Dict[str, Any]], 
                      include_summary=False):
        """Creates a datapackage.yml file for a given dataset"""
        # load & validate recipe
        self.recipe = self._load_recipe(recipe)

        pkg_dir = os.path.realpath(os.path.expanduser(os.path.expandvars(pkg_dir)))


        # for now, resources are first written out, and then "describe_package()" is
        # used to infer the appropriate datapackage metadata;
        #pkg = frictionless.Package()

        for resource_name in resources:
            outfile = os.path.join(pkg_dir, resource_name + ".csv")
            resources[resource_name].to_csv(outfile, index=False)
            # resource = frictionless.describe(resources[resource_name])
            # resource.write(outfile)
            # res = pkg.add_resource(resource)

        pkg = frictionless.describe_package("*.csv", basepath=pkg_dir)

        for i, resource in enumerate(pkg["resources"]):
            dims = resources[resource['name']].shape

            pkg["resources"][i]["num_rows"] = dims[0]
            pkg["resources"][i]["num_columns"] = dims[1]

        now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        mdata = self.recipe

        pkg_uuid = str(uuid.uuid4())
        mdata['uuid'] = pkg_uuid 

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

        # add annotations, if present
        annots = [ ] 

        for annot in annotations:
            annots.append(self.parse_annotation(annot))

        mdata['provenance']['nodes'][pkg_uuid]["annotations"] = annots

        views = [ ] 

        for view in views:
            annots.append(self.parse_view(view))

        mdata['provenance']['nodes'][pkg_uuid]["views"] = views

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

        # write metadata to disk
        with open(os.path.join(pkg_dir, "datapackage.json"), "w") as fp:
            # pretty print for now; can make optional later..
            json.dump(pkg, fp, indent=2, sort_keys=True)

        return pkg

    def parse_annotation(self, annot: str | pathlib.Path):
        """Parses a single annotation"""
        if os.path.exists(annot):
            with open(annot) as fp:
                contents = fp.read()
        elif isinstance(annot, str):
            contents = annot
        else:
            raise Exception("Invalid annotation path specified! {annot}")

        return contents

    def parse_view(self, view: str | pathlib.Path | Dict[str, Any]):
        """Parses a single view"""
        if isinstance(view, (str, pathlib.Path,)) and os.path.exists(view):
            with open(view) as fp:
                contents = json.load(fp)
        else:
            contents = view

        return contents

# TODO: validate existing metadata, when updating dag...
