"""
Io Packager class definition
"""
from __future__ import annotations
import datetime
import frictionless
import json
import os
import pathlib
import sys
import uuid
import yaml
from iodat.summary import compute_summary
from iodat import __version__
from iodat.util import load_tbl
from cerberus import Validator
from typing import Any, Dict, List, Optional
from pkg_resources import resource_filename

class Packager:
    def __init__(self):
        """
        Creates a new Io Packager instance
        """
        self._schema_dir = os.path.abspath(resource_filename(__name__, "profiles"))
        self._version = __version__

    def build_package(self, resources: Dict[str, Any],
                      annotations: Optional[List[str | pathlib.Path]] = [],
                      views: Optional[List[str | pathlib.Path | Dict[str, Any]]] = [],
                      node_metadata: Optional[str | pathlib.Path | Dict[str, Any]] = {},
                      dag_metadata: Optional[str | pathlib.Path | Dict[str, Any]] = {},
                      profile="",
                      pkg_dir="./",
                      include_summary=False):
        """
        Builds an Io datapackage.

        Parameters
        ----------
        resources: dict
            Dictionary of names resources to include in the data package
        annotations: list
            List of filepaths to annotation files, or string annotations
        views: list
            List of filepaths to vega-lite views, or dict representations of such views
        node_metadata: str|path|dict
            [Optional] Additional node-level metadata to include, either as a dict, or path to a yml/json file
        dag_metadata: str|path|dict
            [Optional] Additional DAG-level metadata to include, either as a dict, or path to a yml/json file
        profile: str
            [Optional] Name of metadata profile to use for validation, or "", for none
        pkg_dir: str|path
            [Optional] Location where data package should be saved (default: "./")
        include_summary: bool
            [Optional] Whether or not to compute & embed summary statistics at the time of package
            creation (default: False)
        """
        # parse dag- and node-level metadata
        node_mdata = self._parse_metadata(node_metadata)
        dag_mdata = self._parse_metadata(dag_metadata)

        # if a metadata profile was specified, validate metadata against its schema
        if profile != "":
            self._validate_metadata(node_mdata, profile)

        # determine location to build packages
        pkg_dir = os.path.realpath(os.path.expanduser(os.path.expandvars(pkg_dir)))

        # write resources to output dir
        # for now, resources are first written out, and then "describe_package()" is
        # used to infer the appropriate datapackage metadata;
        for resource_name in resources:
            outfile = os.path.join(pkg_dir, resource_name + ".csv")
            resources[resource_name].to_csv(outfile, index=False)

        # scan package dir to construct initial datapackage
        pkg = frictionless.describe_package("*.csv", basepath=pkg_dir)

        # ideally, it would be preferable to build things up using the already-loaded
        # dataframes, but i haven't found a way to do this in a satisfactory manner,
        # yet.. partial logic for such an approach below: (kh jan 8, 2022)
        #pkg = frictionless.Package()
        #for resource_name in resources:
            # resource = frictionless.describe(resources[resource_name])
            # resource.write(outfile)
            # res = pkg.add_resource(resource)

        # generate a UUID to use for the dataset at this stage in processing
        node_uuid = str(uuid.uuid4())

        # create node metadata block
        node = self.create_node(annotations, views)
        node['metadata'] = node_mdata

        # add io-dag section to data package metadata
        pkg["io-dag"] = {
            "uuid": node_uuid,
            "nodes": {
                node_uuid: node
            },
            "edges": []
        }

        # add num rows/columns to metadata;
        # this may eventually be moved to the optional "compute_summary" section
        #  for i, resource in enumerate(pkg["resources"]):
        #      dims = resources[resource['name']].shape
        #
        #      pkg["resources"][i]["num_rows"] = dims[0]
        #      pkg["resources"][i]["num_columns"] = dims[1]

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
            json.dump(pkg, fp, indent=2, sort_keys=True)

    def update_package(self, existing: str | pathlib.Path, 
                       resources: Dict[str, Any],
                       annotations: Optional[List[str | pathlib.Path]] = [],
                       views: Optional[List[str | pathlib.Path | Dict[str, Any]]] = [],
                       metadata: Optional[str | pathlib.Path | Dict[str, Any]] = {},
                       profile="",
                       pkg_dir="./",
                       include_summary=False,
                       **kwargs):
        """
        Updates an existing Io datapackage.

        Parameters
        ----------
        existing: str|path
            Path to existing datapackage.json to be extended
        resources: dict
            Dictionary of names resources to include in the data package
        annotations: list
            List of filepaths to annotation files, or string annotations
        views: list
            List of filepaths to vega-lite views, or dict representations of such views
        metadata: str|path|dict
            [Optional] Additional metadata to include, either as a dict, or path to a yml/json file
        profile: str
            [Optional] Name of metadata profile to use for validation, or "", for none
        pkg_dir: str|path
            [Optional] Location where data package should be saved (default: "./")
        include_summary: bool
            [Optional] Whether or not to compute & embed summary statistics at the time of package
            creation (default: False)
        **kwargs: dict
            [Optional] Additional global/DAG-level metadata to be included
        """
        # determine location to build packages
        pkg_dir = os.path.realpath(os.path.expanduser(os.path.expandvars(pkg_dir)))

        # parse user-provided processing stage (node-level) metadata
        node_mdata = self._parse_metadata(metadata)

        # if a metadata profile was specified, validate metadata against its schema
        if profile != "":
            self._validate_metadata(node_mdata, profile)

        # write resources to output dir
        # for now, resources are first written out, and then "describe_package()" is
        # used to infer the appropriate datapackage metadata;
        for resource_name in resources:
            outfile = os.path.join(pkg_dir, resource_name + ".csv")
            resources[resource_name].to_csv(outfile, index=False)

        # scan package dir to construct initial datapackage
        pkg = frictionless.describe_package("*.csv", basepath=pkg_dir)

        node = self.create_node(annotations, views, action='update_package')
        node['metadata'] = node_mdata

        # extract io metadata + dag from existing data package
        if not os.path.exists(existing):
            raise Exception(f"Invalid path to existing data package provided: {existing}")

        with open(existing) as fp:
            existing_mdata = json.load(fp)

        # generate a UUID to use for the dataset at this stage in processing
        node_uuid = str(uuid.uuid4())

        # copy over metadata section from previous stage
        pkg["io-dag"] = existing_mdata["io-dag"]

        # store uuid of previous step, and update
        prev_uuid = pkg["io-dag"]["uuid"]
        pkg["io-dag"]["uuid"] = node_uuid

        # update provenance DAG
        pkg["io-dag"]["nodes"][node_uuid] = node
        pkg["io-dag"]["edges"].append({
            "source": prev_uuid,
            "target": node_uuid
        })

        # write metadata to disk
        with open(os.path.join(pkg_dir, "datapackage.json"), "w") as fp:
            json.dump(pkg, fp, indent=2, sort_keys=True)

    def create_node(self, annotations: List[str], views: List[Dict[str, Any]], action='build_package'):
        """
        Generates metadata block for a new node in the DAG

        Parameters
        ---------
        annotations: list
            List of filepaths to annotation files, or string annotations
        views: list
            List of filepaths to vega-lite views, or dict representations of such views
        """
        now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        node = {
            "name": "io-python",
            "action": action,
            "time":  now,
            "version": self._version,
            "annot": [],
            "views": []
        }

        # add annotations and views, if present
        for annot in annotations:
            node["annot"].append(self.parse_annotation(annot))

        for view in views:
            node["views"].append(self.parse_view(view))

        return node

    def _parse_metadata(self, mdata: str | pathlib.Path | Dict[str, Any]) -> Dict[str, Any]:
        """
        Parses metadata and validates it against a specific profile, if specified

        Parameters
        ----------
        mdata: str|path|dict
            Path to metadata yml|json, or, a dict representation

        Returns
        -------
        dict
            Metadata dict
        """
        # if path specified, attempt to load mdata from file
        if isinstance(mdata, (str, pathlib.Path,)):
            if not os.path.exists(mdata):
                raise FileNotFoundError("Unable to find metadata at specified location!")

            with open(mdata) as fp:
                mdata = yaml.load(fp, Loader=yaml.FullLoader)

        assert isinstance(mdata, dict)

        return mdata

    def _validate_metadata(self, mdata: Dict[str, Any], profile: str):
        """
        Validates metadata against a particular profile known to Io

        NOTE (jan 8, 2022): at present, this functionality is directed at specific
        "known" metadata profiles.
        This is intended to provide a means by which tooling can be created for
        interacting with data which contains useful metadata in a particular format.
        In the future, this will be modified & related so that external validation
        schemas can be provided as well.

        Parameters
        ----------
        mdata: dict
            Metadata to be validated
        profile: str
            Name of the profile to use for validation
        """
        # load base "io" profile
        with open(os.path.join(self._schema_dir, "io.yml")) as schema_fp:
            schema = yaml.load(schema_fp, Loader=yaml.FullLoader)

        # if an alternate profile is specified, extend base io schema with the
        # supported fields associated with that profile
        if "profile" != "io":
            if profile not in schema['profile']['allowed']:
                raise Exception("Unrecognized profile specified!")

            # load profile validation schema and append to base schema
            infile = os.path.join(self._schema_dir, f"{profile}.yml")

            with open(infile) as profile_fp:
                profile_schema = yaml.load(profile_fp, Loader=yaml.FullLoader)

                schema[profile] = {
                    "type": 'dict',
                    "required": True,
                    "schema": profile_schema
                }

        # validate metadata against the loaded schema
        validator = Validator(schema)

        if not validator.validate(mdata, schema):
            print(f"mdata validation failed!")
            print(validator.errors)
            sys.exit(1)

    def parse_annotation(self, annot: str | pathlib.Path):
        """
        Parses a single annotation

        Parameters
        ----------
        annot: str|path
            Either an annotation string, or a path to a plain-text file

        Returns
        -------
        str
            annotation string
        """
        if os.path.exists(annot):
            with open(annot) as fp:
                contents = fp.read()
        else:
            if isinstance(annot, pathlib.Path):
                raise Exception(f"Invalid annotation path provided: {annot}")

            contents = annot

        return contents

    def parse_view(self, view: str | pathlib.Path | Dict[str, Any]):
        """
        Parses a single view

        Parameters
        ----------
        view: str|path|dict
            either a vega-lite view loaded into a dict, or a path to a json file
            containg such a view

        Returns
        -------
        dict
            vega-lite view dict
        """
        if isinstance(view, (str, pathlib.Path,)) and os.path.exists(view):
            with open(view) as fp:
                contents = json.load(fp)
        else:
            contents = view

        return contents

