"""
Packager class definition
"""
import datetime
import frictionless
import glob
import os
import shortuuid
import yaml
import numpy as np
import pandas as pd
from iodat.summary import compute_summary
from cerberus import Validator
from pkg_resources import resource_filename


class Packager:
    def __init__(self, recipe):
        """
        Creates a new Packager instance

        Arguments
        ---------
        recipe: str
            Path to data recipe yml
        """
        self._conf_dir = os.path.abspath(resource_filename(__name__, "conf"))
        self._schema_dir = os.path.abspath(resource_filename(__name__, "schema"))

        # load system config
        self._config = self._load_io_config()

        # load & validate recipe
        self.recipe = self._load_recipe(recipe)

        # load data model config files
        self.analyses = self._load_config("analyses")
        self.assays = self._load_config("assays")
        self.platforms = self._load_config("platforms")

    def _load_recipe(self, path):
        """Validates io recipe"""
        # load recipe
        with open(path) as fp:
            recipe = yaml.load(fp, Loader=yaml.FullLoader)

        # load schema
        with open(os.path.join(self._schema_dir, "recipe.yml")) as schema_fp:
            schema = yaml.load(schema_fp, Loader=yaml.FullLoader)

        validator = Validator(schema)

        if validator.validate(recipe, schema) is not True:
            print(f"Recipe validation failed!")
            print(validator.errors)
            sys.exit(1)

        return recipe

    def _load_io_config(self):
        """
        Loads Io base configuration.

        Used to determine base package output directory, and contributors info.
        """
        infile = os.path.join(os.getenv("XDG_CONFIG_HOME"), "io", "config.yml")

        if not os.path.exists(infile):
            print(f"[Error] cannot find config file at {infile}!")
            sys.exit(1)

        with open(infile) as fp:
            return yaml.load(fp, Loader=yaml.FullLoader)

    def get_output_dir(self):
        """Returns path to the base system-wide datapackage output dir"""
        return os.path.join(
            self._config["output_dir"], self._config["version"], self.recipe["id"]
        )

    def _load_config(self, target):
        """Loads a yaml configuration file specifying a component in the Io data
        model."""
        infile = os.path.join(self._conf_dir, "other", target + ".yml")

        with open(infile) as fp:
            cfg = yaml.load(fp, Loader=yaml.FullLoader)

            # load and validate schema
            with open(os.path.join(self._schema_dir, f"{target}.yml")) as schema_fp:
                schema = yaml.load(schema_fp, Loader=yaml.FullLoader)
                validator = Validator(schema)

                if validator.validate(cfg, schema) is not True:
                    print(f"Validation failed for config: {infile}!")
                    print(validator.errors)
                    sys.exit(1)

                return cfg[target]

    def build_package(self):
        """Creates a datapackage.yml file for a given dataset"""
        # cd to the directory containing the package data;
        # at present, it's not possible to parse files outside of direct/child directories
        pkg_dir = self.get_output_dir()
        os.chdir(pkg_dir)

        # create a new DataPackage instance and set relevant fields
        pkg = frictionless.describe_package("*.tsv")

        # add data type to resource descriptors
        for i, resource in enumerate(pkg["resources"]):
            fname = os.path.basename(resource["path"])

            if fname == "data.tsv":
                pkg["resources"][i]["type"] = "dataset"
            elif fname == "row-metadata.tsv":
                pkg["resources"][i]["type"] = "row-metadata"
            elif fname == "col-metadata.tsv":
                pkg["resources"][i]["type"] = "column-metadata"
            else:
                breakpoint()
                raise Exception(
                    f"Unrecognized resource encountered: {resource['path']}"
                )

        # custom metadata section to add to datapackage yml
        mdata = {
            "data": {
                "dataset": {"id": self.recipe["id"], "title": self.recipe["title"]},
                "datasource": self.recipe["datasource"],
                "processing": self.recipe["processing"],
            },
            "datatype": self.recipe["datatype"],
            "contributors": self._config["metadata"]["contributors"],
            "uuid": shortuuid.ShortUUID().random(length=8),
            "rows": self.recipe["rows"]["name"],
            "columns": self.recipe["columns"]["name"],
            "provenance": self.recipe["provenance"],
        }

        # add experiment (optional)
        if "experiment" in self.recipe:
            mdata["data"]["experiment"] = self.recipe["experiment"]

        # add analysis (optional)
        if "analysis" in self.recipe:
            analysis = self.recipe["analysis"]

            if not analysis in self.analyses:
                raise ValueError(f"Unrecognized analysis specified: {analysis}")

            mdata["analysis"] = self.analyses[analysis].copy()
            mdata["analysis"]["name"] = analysis

        # add assay (optional)
        if "assay" in self.recipe:
            assay = self.recipe["assay"]

            if not assay in self.assays:
                raise ValueError(
                    f"Unrecognized assay specified: {self.recipe['assay']}"
                )

            mdata["assay"] = self.assays[assay].copy()
            mdata["assay"]["name"] = assay

        # add platform (optional)
        if "platform" in self.recipe:
            platform = self.recipe["platform"]

            if not platform in self.platforms:
                raise ValueError(
                    f"Unrecognized platform specified: {self.recipe['platform']}"
                )

            mdata["platform"] = self.platforms[platform].copy()
            mdata["platform"]["name"] = platform
            # del mdata["platform"]["assays"]

        if "fields" in self.recipe:
            mdata["fields"] = self.recipe["fields"]

        # add data packager provenance entry
        now = datetime.datetime.utcnow()

        prov = {
            "time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "action": "io-datapackager",
            "version": self._config["version"],
        }
        mdata["provenance"].append(prov)

        # add io metadata
        pkg["io"] = mdata

        # add summary statistics
        try:
            data_path = os.path.join(pkg_dir, "data.tsv")
            pkg["summary"] = compute_summary(data_path)
        except:
            raise RuntimeError("Error computing dataset summary")

        return pkg
