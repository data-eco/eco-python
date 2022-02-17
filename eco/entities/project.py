"""
Eco Project class definition
"""
from __future__ import annotations
import datetime
import os
import pathlib
import yaml
from rich import print
from typing import Any, Dict, List, Optional

class Project:
    """
    Class representing a project

    Attributes
    ----------
    id: str
        Identifier used to refer to project (must be unique)
    name: str
        Project name
    description: str
        Project description
    code: list
        List of paths to analyses/code relating to the project
    datasets: list
        List of paths to datasets relating to the project
    notes: list
        List of paths to notes relating to the project
    exclude: list
        List of strings to use for filtering project assets; any paths containing 
        one or more of the exclude terms will be excluded from all results
    """
    def __init__(self, id:str, name="", description="",
                 code: List[str | pathlib.Path] = [],
                 datasets: List[str | pathlib.Path] = [],
                 notes: List[str | pathlib.Path] = [],
                 exclude: List[str] = []):
        self.id = id
        self.name = name
        self.description = description

        # supported file formats
        # note: this logic will eventually be generalized
        self._file_formats = {
            "code": [".py", ".ipynb", ".jl", ".R", ".Rmd", ".r", ".html"],
            "datasets": [".csv", ".csv.gz", ".tsv", ".tsv.gz", ".arrow", ".feather", ".parquet"],
            "notes": [".md"]
        }

        self.exclude = exclude

        self.code = self._detect_assets("code", code)
        self.datasets = self._detect_assets("datasets", datasets)
        self.notes = self._detect_assets("notes", notes)

    def _detect_assets(self, format:str, asset_paths: List[str | pathlib.Path] = []):
        """
        Detects project-related assets of different types
        """
        assets:List[str] = []

        known_ext = self._file_formats[format]

        for target_path in asset_paths:
            target_path = os.path.realpath(os.path.expanduser(os.path.expandvars(target_path)))

            # if path corresponds to a file, simply add it to the list
            if os.path.isfile(target_path):
                if not os.path.splitext(target_path)[1] in known_ext:
                    raise Exception("Unrecognized {format} file format for file: {path}")
                assets.append(target_path)
                continue

            # otherwise, recursively scan directories 
            paths = list(pathlib.Path(target_path).glob("**/*.*"))

            for path in paths:
                if os.path.splitext(path)[1] in known_ext:
                    str_path = path.as_posix()
                    if len([x for x in self.exclude if x in str_path]) == 0:
                        assets.append(str_path)

        # return a list of dicts with path + date modified time for each asset
        asset_dicts = [{'path': x, 'mtime': os.stat(x).st_mtime} for x in assets]
        asset_dicts.sort(key=lambda x: x['mtime'], reverse=True)

        return asset_dicts


    def info(self, num_recent=3):
        """
        Prints project summary info to the screen

        Parameters
        ----------
        num_recent: int
            Number of recently modified files of each type to display
        """
        divider = "[cyan]" + 80 * '=' + "[/cyan]"
        print(divider)
        print(f"[bold light_coral]{self.name}[/bold light_coral]") 
        print(f"[light_coral]{self.description}[/light_coral]")
        print(divider)

        # code section
        num_code_assets = len(self.code)

        if num_code_assets > 0:
            print(f"[bold steel_blue1]Code ({num_code_assets}):[/bold steel_blue1]")

            for code_asset in self.code[:min(num_recent, num_code_assets)]:
                asset_date = datetime.datetime.fromtimestamp(code_asset['mtime'])
                date_str = asset_date.strftime("%a %b %d")

                print(f"- {code_asset['path']} ([cyan2]{date_str}[/cyan2])")

        # data section
        num_datasets = len(self.datasets)

        if num_datasets > 0:
            print(f"[bold steel_blue1]Data ({num_datasets}):[/bold steel_blue1]")

            for data_asset in self.datasets[:min(num_recent, num_datasets)]:
                asset_date = datetime.datetime.fromtimestamp(data_asset['mtime'])
                date_str = asset_date.strftime("%a %b %d")

                print(f"- {data_asset['path']} ([cyan2]{date_str}[/cyan2])")

        # notes section
        num_notes = len(self.notes)

        if num_notes > 0:
            print(f"[bold steel_blue1]Notes ({num_notes}):[/bold steel_blue1]")

            for note in self.notes[:min(num_recent, num_notes)]:
                asset_date = datetime.datetime.fromtimestamp(note['mtime'])
                date_str = asset_date.strftime("%a %b %d")

                print(f"- {note['path']} ([cyan2]{date_str}[/cyan2])")

    @staticmethod
    def from_yml(path):
        """
        Creates a new Project instance from a specified yaml config file.

        Parameters
        ----------
        path: str|Path
            path to an eco project yaml config file

        Return
        ------
        out: Project
        """
        if not os.path.exists(path):
            raise Exception("Unable to find project config file at specified location")
        elif not os.path.splitext(path)[1] in [".yml", ".yaml"]:
            raise Exception("Invalid file extension for specified project config file")

        with open(path) as fp:
            cfg = yaml.load(fp, Loader=yaml.FullLoader)

        # todo: validate config schema
        return Project(**cfg)

    @staticmethod
    def from_dict(dict_):
        """
        Creates a new Project instance from a dictionary

        Parameters
        ----------
        cfg: dict
            Dictionary representation of a project config

        Return
        ------
        out: Project
        """
        return Project(**dict_)

