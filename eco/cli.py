"""
Eco CLI
"""
import logging
import os
import sys
import yaml
import pandas as pd
from textwrap import wrap
from argparse import ArgumentParser
from rich import print
from typing import Any, Dict, List, Optional
from eco.entities.datapackage import DataPackage
from eco.entities.project import Project

class CLI:
    """Eco Command-line Interface class"""

    def __init__(self):
        """Initializes a new CLI instance"""
        self._conf_dir = self._get_conf_dir()

        cmd = self._get_cmd()

        self._setup_logger()
        self._load_config()

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
        args, unknown = parser.parse_known_args(self._cmd_args)

        pkg_path = self._parse_pkg_path(args.path)
        pkg_dir = os.path.dirname(pkg_path)

        pkg = DataPackage(pkg_path)

        # print header block with dataset title, id, and source
        divider = "[cyan]" + 80 * '=' + "[/cyan]"
        title = "\n".join(wrap(pkg.dataset['title'], 80))

        print(divider)
        print(f"[bold light_coral]{pkg.dataset['id']}[/bold light_coral]") 
        print(f"[light_coral]{pkg.source['title']}[/light_coral]")
        print(f"[bold white]{title}[/bold white]")

        #  description = "\n".join(wrap(pkg.provenance['description'], 80))
        #  print()
        #  print(f"[white]{description}[/white]")

        print(divider)

        # rows/columns
        resource = pkg.get_resource_info()
        num_cols = len(resource['schema']['fields'])

        print("[bold steel_blue1]Overview[/bold steel_blue1]")
        print(f" columns: {num_cols} ({pkg.columns})")
        print(f" rows: ? ({pkg.rows})")

        # print dag overview
        num_nodes = len(pkg.dag['nodes'])
        num_edges = len(pkg.dag['edges'])

        print("[bold steel_blue1]DAG:[/bold steel_blue1]")
        print(f" nodes: {num_nodes}")
        print(f" edges: {num_edges}")

        # print view info
        view_names = pkg.get_view_names()

        if len(view_names) > 0:
            print("[bold steel_blue1]Views:[/bold steel_blue1]")
            
            for view in view_names:
                print(f"- {view}")

        # check for biodat, etc. profile and render profile-specific info?

        #  "assay": "microarray",
        #  "datatype": "experimental_dataset",
        #  "platforms": "GPL25401",
        #  "sample_types": "patient",
        #  "diseases": [
        #    "D009101",
        #    "D000075122"
        #  ],
        #  "species": 9606
        if pkg.profile == 'biodat':
            species = pkg.biodat['species']
            if isinstance(species, list):
                species = ", ".join(species)

            diseases = pkg.biodat['diseases']
            if isinstance(diseases, list):
                diseases = ", ".join(diseases)

            print("[bold spring_green3]BioDat[/bold spring_green3]", ":dna:")
            print(f" Assay: [spring_green3]{pkg.biodat['assay']}[/spring_green3]")
            print(f" Species: [spring_green3]{species}[/spring_green3]")

            if diseases != "":
                print(f" Diseases: [spring_green3]{diseases}[/spring_green3]")

    def proj(self):
        """Project-related commands"""
        # parse "proj"-specific args
        parser = ArgumentParser(description='Eco project information')

        parser.add_argument(
            "project",
            nargs="?",
            type=str,
            default="",
            help='Project ID',
        )

        # parse remaining parts of command args
        args, unknown = parser.parse_known_args(self._cmd_args)

        # load project configs
        proj_cfg_dir = os.path.join(os.path.dirname(self.config_path), "projects")

        projects = {}

        for file in os.listdir(proj_cfg_dir):
            if os.path.splitext(file)[1] in ['.yml', '.yaml']:
                with open(os.path.join(proj_cfg_dir, file)) as fp:
                    cfg = yaml.load(fp, Loader=yaml.FullLoader)
                    projects[cfg['id']] = Project.from_dict(cfg)

        self.projects = projects

        # if project id specified, check to make sure its valid
        if args.project != "":
            valid_ids = projects.keys()

            if args.project not in valid_ids:
                msg = f"Unknown project id specified! Valid choices are: {', '.join(valid_ids)}"
                raise Exception(msg)

            projects[args.project].info()

    def viz(self):
        """Visualize datapackage view"""
        # parse "info"-specific args
        parser = ArgumentParser(description='Visualize datapackage view')

        parser.add_argument(
            "path",
            nargs="?",
            type=str,
            default=".",
            help='Path to data package (default: "./")',
        )

        parser.add_argument(
            "--view",
            type=str,
            help='Name of view to be rendered (defaults to first view)',
        )

        # parse remaining parts of command args
        args, unknown = parser.parse_known_args(self._cmd_args)

        pkg_path = self._parse_pkg_path(args.path)
        pkg_dir = os.path.dirname(pkg_path)

        pkg = DataPackage(pkg_path)

        pkg.plot(view_name=args.view)

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
        # if "ECO_CONF_DIR" environment variable is set, use it
        if "ECO_CONF_DIR" in os.environ:
            conf_dir = os.environ.get("ECO_CONF_DIR")
        # config dir (linux / osx)
        else:
            conf_dir = os.getenv("XDG_CONFIG_HOME",
                                 os.path.expanduser("~/Library/Preferences/eco"))

        if not os.path.exists(conf_dir):
            raise Exception("Unable to find system configuration dir! Are you sure you are running a supported OS?..")

        return conf_dir

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
   proj     Project related commands
   search   Search Eco repositories
   rows     Show row summary
   cols     Show column summary
   viz      Dataset visualization
''')

# text for possible future commands
#  view     Eco view commands
#  summary  Show summary

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

        valid_cmds = ['info', 'proj', 'search', 'viz']

        if args.command not in valid_cmds:
            print(f"[ERROR] Unrecognized command specified: {args.command}!")
            parser.print_help()
            sys.exit()

        # store remaining non-global arguments
        global_args = []
        self._cmd_args = [x for x in sys.argv[2:] if x not in global_args]

        # execute method with same name as sub-command
        return args.command

    def _load_config(self):
        """Loads main eco config"""
        with open(self.config_path) as fp:
            self.config = yaml.load(fp, Loader=yaml.FullLoader)

    def _setup_logger(self):
        """Sets up logger to print messages to STDOUT"""
        logging.basicConfig(stream=sys.stdout, 
                            format='[%(levelname)s] %(message)s')

        self._logger = logging.getLogger('Eco')

        if self.verbose:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.WARN)
