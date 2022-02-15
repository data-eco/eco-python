"""
Eco CLI
"""
import logging
import json
import os
import sys
import yaml
import tempfile
import pandas as pd
import seaborn as sns
from argparse import ArgumentParser
from rich import print
from typing import Any, Dict, List, Optional

class CLI:
    """Eco Command-line Interface class"""

    def __init__(self):
        """Initializes a new CLI instance"""
        self._conf_dir = self._get_conf_dir()

        self._print_header()

        cmd = self._get_cmd()

        self._setup_logger()

        getattr(self, cmd)()

    def info(self):
        """
        "info" command
        """
        # parse "info"-specific args
        parser = ArgumentParser(description='Show info for data package')

        parser.add_argument(
            "path",
            nargs="?",
            type=str,
            default=".",
            help='Path to data package (default: "./")',
        )

        # parse remaining parts of command args
        input_args = [x for x in sys.argv[1:] if x != "info"]
        args, unknown = parser.parse_known_args(input_args)

        pkg_path = self._parse_pkg_path(args.path)
        pkg_dir = os.path.dirname(pkg_path)

        pkg = self._load_pkg(pkg_path)

        mdat = pkg['eco']['metadata']

        #steel_blue1
        print(f"[bold white]{mdat['data']['dataset']['title']}[/bold white]")
        print(f"[bold steel_blue3]{mdat['data']['dataset']['id']}[/bold steel_blue3] ([light_coral]{mdat['data']['source']['title']}[/light_coral])")

        # print dag overview
        num_nodes = len(pkg['eco']['nodes'])
        num_edges = len(pkg['eco']['edges'])

        print("[bold steel_blue1]Provenance DAG:[/bold steel_blue1]")
        print(f"- nodes: {num_nodes}")
        print(f"- edges: {num_edges}")

        # row/column numbers/ids?
        # dag / annots / view summary?

        # check for biodat, etc. profile and render profile-specific info?

    def _parse_pkg_path(self, path):
        """
        Validates a specified data package path and returns a path to the the
        corresponding datapackage json/yaml file.

        Arguments
        ---------
        path: str
            User-specified data package location

        Return
        ------
        path: str
            Location to a valid json/yaml datapackage metadata file
        """
        # validate path
        pkg_path = os.path.realpath(os.path.expanduser(os.path.expandvars(path)))

        valid_filenames = ["datapackage.json", "datapackage.yml", "datapackage.yaml"]

        if not os.path.exists(pkg_path):
            self._logger.error(f"Unable to find data package at the specified location: {pkg_path}")
            sys.exit()

        # check for presence of a valid data package
        if os.path.isdir(pkg_path):
            files = os.listdir(pkg_path)

            pkg_file = None

            for file in files:
                if file in valid_filenames:
                    pkg_file = file

            if pkg_file is None:
                self._logger.error(f"Unable to find data package in the specified directory: {pkg_path}")
                sys.exit()

            pkg_path = os.path.join(pkg_path, pkg_file)

        else:
            pkg_file = os.path.basename(pkg_path)

            if pkg_file not in valid_filenames:
                self._logger.error(f"Invalid data package path specified: {pkg_path}")
                sys.exit()

        return pkg_path

    def _load_pkg(self, pkg_path: str) -> Dict[str, Any]:
        # load metadata
        with open(pkg_path) as fp:
            if pkg_path.endswith("json"):
                pkg = json.load(fp)
            else:
                pkg = yaml.load(fp, Loader=yaml.FullLoader)

        return pkg

    def _get_resource(self, pkg_dir: str, pkg: Dict[str, Any], index=0) -> pd.DataFrame:
        """
        load specified data package resource
        """
        # validate index..

        # get path to target resource
        path = os.path.join(pkg_dir, pkg['resources'][index]['path'])

        if path.endswith(".csv"):
            return pd.read_csv(path)
        elif path.endswith(".tsv"):
            return pd.read_csv(path, sep='\t')
        elif path.endswith(".feather"):
            return pd.read_feather(path)

    def _get_conf_dir(self) -> str:
        """determines config dir to use or raises an error if none found"""
        # config dir (linux / osx)
        conf_dir = os.getenv("XDG_CONFIG_HOME",
                             os.path.expanduser("~/Library/Preferences/"))

        if not os.path.exists(conf_dir):
            raise Exception("Unable to find system configuration dir! Are you sure you are running a supported OS?..")

        return os.path.join(conf_dir, "eco")

    def _get_cmd(self):
        """
        Parses command line arguments and determine command to run + any global
        arguments.

        Based on: https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
        """
        parser = ArgumentParser(
            description="Eco",
            usage='''eco <command> [<args>]

List of supported commands:
   info     Show info for data package
   search   Search Eco repositories
   rows     Show row summary
   cols     Show column summary
   summary  Show overall summary
   view     Eco view commands
   viz      Dataset visualization
''')

        parser.add_argument('command', help='Sub-command to run')

        # default config file location
        config = os.path.join(self._conf_dir, "config.yml")

        parser.add_argument(
            "--config",
            help=f"Path to Eco config file to use (default: {config})",
            default=config,
        )

        parser.add_argument(
            "--verbose",
            help="If enabled, prints verbose output",
            action="store_true",
        )
        
        # parse and validate sub-command
        args, unknown = parser.parse_known_args()

        self.verbose = args.verbose
        self.config_path = args.config

        valid_cmds = ['info', 'dag', 'search', 'summary', 'view', 'viz']

        if args.command not in valid_cmds:
            print(f"[ERROR] Unrecognized command specified: {args.command}!")
            parser.print_help()
            sys.exit()

        # execute method with same name as sub-command
        return args.command

    def _print_header(self):
        """
        prints Eco header
        """
        print("[cyan]========================================[/cyan]")
        print(":microscope:", "[bold spring_green2]Eco[/bold spring_green2]")
        print("[cyan]========================================[/cyan]")

    def _setup_logger(self):
        """Sets up logger to print messages to STDOUT"""
        logging.basicConfig(stream=sys.stdout, 
                            format='[%(levelname)s] %(message)s')

        self._logger = logging.getLogger('Eco')

        if self.verbose:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.WARN)
