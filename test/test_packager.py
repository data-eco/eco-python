import os
import tempfile
import json
import pathlib

import pytest
import pandas

import eco


class TestPackager:

    @pytest.fixture
    def packager(self):
        from eco.packager import Packager
        return Packager()

    def test_packager(self, packager):
        assert packager._version == eco.__version__

    @pytest.fixture
    def empty_data(self):
        return {'test_data': pandas.DataFrame()}

    def test_build_package(self, packager, empty_data):
        with tempfile.TemporaryDirectory() as pkg_dir:
            packager.build_package(empty_data, pkg_dir=pkg_dir)
            files = os.listdir(pkg_dir)
            assert 'datapackage.json' in files

    @staticmethod
    def check_vals(vals, val_name, load_fn, pkg_dir):
        with open(os.path.join(pkg_dir, 'datapackage.json')) as j:
            data = json.load(j)
            uuid = data['eco']['uuid']
            read_vals = data['eco']['nodes'][uuid][val_name]
            all_good = True
            if val_name in ('metadata',):
                vals, read_vals = [vals], [read_vals]

            for val, read_val in zip(vals, read_vals):
                if isinstance(val, pathlib.Path):
                    with open(val) as i:
                        read_valf = load_fn(i)
                        if not read_val == read_valf:
                            all_good = False
                elif not val == read_val:
                    all_good = False

            assert all_good, "Read view does not match original content"

    @pytest.mark.parametrize("annotations",
                             [
                                 ['annotation 1'],
                                 ['annotation 1', 'annotation 2'],
                                 [pathlib.Path('annot/fetch_data/overview.md')]
                             ])
    def test_build_package_annotations(self, packager, empty_data, annotations):
        with tempfile.TemporaryDirectory() as pkg_dir:
            packager.build_package(empty_data, annotations=annotations, pkg_dir=pkg_dir)
            self.check_vals(annotations, 'annot', lambda i: i.read(), pkg_dir)

    @pytest.mark.parametrize("views",
                             [
                                 [{'test': 'view'}],
                                 [pathlib.Path('views/fetch_data/scatterplot.json')],
                             ])
    def test_build_package_views(self, packager, empty_data, views):
        with tempfile.TemporaryDirectory() as pkg_dir:
            packager.build_package(empty_data, views=views, pkg_dir=pkg_dir)
            self.check_vals(views, 'views', json.load, pkg_dir)

    @pytest.mark.parametrize("metadata",
                             [
                                 {'test': [1,2,3]},
                                 pathlib.Path('metadata/test_metadata.json'),
                             ])
    def test_build_package_nodemetadata(self, packager, empty_data, metadata):
        with tempfile.TemporaryDirectory() as pkg_dir:
            packager.build_package(empty_data, metadata=metadata, pkg_dir=pkg_dir)
            self.check_vals(metadata, 'metadata', json.load, pkg_dir)

    @pytest.mark.parametrize("metadata",
                             [
                                 {'test': [1,2,3]},
                             ])
    def test_build_package_pkgmetadata(self, packager, empty_data, metadata):
        with tempfile.TemporaryDirectory() as pkg_dir:
            packager.build_package(empty_data, pkg_dir=pkg_dir, **metadata)
            with open(os.path.join(pkg_dir, 'datapackage.json')) as j:
                data = json.load(j)
                read_metadata = {key: data['eco'][key] for key in metadata.keys()}
                assert read_metadata == metadata
