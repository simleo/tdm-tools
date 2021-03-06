# Copyright 2018-2019 CRS4
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime
from urllib.parse import urljoin
import io
import json
import os

import gdal
import osr
import xarray as xr

from tdm import __version__ as version

gdal.UseExceptions()


def to_datetime(t):
    ns = 1e-9  # nanosecs in a sec
    FMT = '%Y-%m-%d_%H:%M:%S'
    return datetime.utcfromtimestamp(t.values.astype(int) * ns).strftime(FMT)


def to_coord_range(coord):
    vals = coord.values
    return '{}:{}:{:.3g}'.format(
        vals[0], len(vals), (vals[-1] - vals[0])/len(vals))


def create_res_description(name, desc, url, dt, lonlat, fname):
    stat = os.stat(fname)
    return {"name": name,
            "description": desc,
            "url": url,
            "format": "TIFF",
            "mimetype": "image/tiff",
            "created": stat.st_ctime,
            "last_modified": stat.st_mtime,
            "size": stat.st_size,
            }


class MemMasterBuilder(object):
    def __init__(self, lons, lats):
        self.driver = gdal.GetDriverByName("MEM")
        self.geotransform = self.get_geotransform(lons, lats)

    # NOTE: it is coordinated with the permutation that we impose in build()
    def get_geotransform(self, lons, lats):
        return (lons[0], (lons[-1] - lons[0])/len(lons), 0,
                lats[-1], 0, -(lats[-1] - lats[0])/len(lats))

    def setup_raster(self, data):
        rows, cols = data[0].shape
        raster = self.driver.Create("", cols, rows, len(data),
                                    gdal.GDT_Float32)
        raster.SetGeoTransform(self.geotransform)
        raster_srs = osr.SpatialReference()
        raster_srs.ImportFromEPSG(4326)
        raster.SetProjection(raster_srs.ExportToWkt())
        return raster

    def build(self, data, metadata):
        if not isinstance(data, list):
            data = [data]
        raster = self.setup_raster(data)
        for i, d in enumerate(data):
            band = raster.GetRasterBand(1 + i)
            band.WriteArray(d[::-1, :])
            band.FlushCache()
        return raster


class PathBuilder(object):
    def __init__(self, dir_root, desc):
        self.product_root = os.path.join(
            dir_root, 'tdm/odata/product', desc['group'],
            desc['class'], desc['name'], desc['uid'])

    def build(self, *args):
        return os.path.join(self.product_root, *args)


def get_simulation_details(args, dataset):
    # This simulates the result of a query to the simulations db
    times = dataset.coords['time']
    lons = dataset.coords['lon']
    lats = dataset.coords['lat']
    # FIXME history should contain the relevant provenance
    # details when we have it
    basename, ext = os.path.splitext(os.path.basename(args.nc_path))
    parts = basename.split('_')
    pclass = parts[0]
    puid = parts[-1]
    pname = '_'.join(parts[1:-1])
    pgroup = args.product_group if args.product_group else 'simulation'
    pclass = args.product_class if args.product_class else pclass
    puid = args.instance_uid if args.instance_uid else puid
    simulation_details = {
        'group': pgroup,
        'class': pclass,
        'name': pname,
        'uid': puid,
        'path': args.nc_path,
        'history': [f'Extracted by tdm map_to_tree {version}'],
        'start_time': to_datetime(times[0]),
        'end_time': to_datetime(times[-1]),
        'lon_range': to_coord_range(lons),
        'lat_range': to_coord_range(lats)
    }
    return simulation_details


def dump_to_tree(out_dir, dataset, simulation_details, url_root):
    times = dataset.coords['time']
    lons = dataset.coords['lon']
    lats = dataset.coords['lat']
    out_driver = gdal.GetDriverByName("GTiff")
    rbuilder = MemMasterBuilder(lons, lats)
    pbuilder = PathBuilder(out_dir, simulation_details)
    lonlat = '_'.join(simulation_details[x + '_range'] for x in ['lon', 'lat'])
    resources = []
    for t in times:
        ds = dataset.sel({'time': t})
        ts = to_datetime(t)
        path = pbuilder.build(ts, lonlat)
        os.makedirs(path, exist_ok=True)
        for fname, desc, fdata in [
            ('tcov', "Total cloud coverage [percent]",
             [ds['TCDC_surface'].values]),
            ('tprec', "Total precipitation [kg/m^2]",
             [ds['APCP_surface'].values]),
            ('temp2m', "Temperature 2m above ground [C]",
             [ds['TMP_2maboveground'].values - 273.15]),
            ('uv10', "Wind velocity at 10m [m/s]",
             [ds['UGRD_10maboveground'].values,
              ds['VGRD_10maboveground'].values])]:
            raster = rbuilder.build(fdata, {"TIFFTAG_DATETIME": ts})
            out_path = os.path.join(path, "%s.tif" % fname)
            out_driver.CreateCopy(out_path, raster)
            url = urljoin(url_root, out_path)
            resources.append(create_res_description(fname, desc, url, ts,
                                                    lonlat, out_path))
            print('created %s' % out_path)
    desc = {'description': simulation_details,
            'result': {"resources": resources}}
    desc_path = pbuilder.build("description.json")
    with io.open(desc_path, "wt") as f:
        f.write(json.dumps(desc, indent=4, sort_keys=False))


def main(args):
    dataset = xr.open_dataset(args.nc_path)
    simulation_details = get_simulation_details(args, dataset)
    dump_to_tree(args.out_dir, dataset, simulation_details, args.url_root)


def add_parser(subparsers):
    parser = subparsers.add_parser("map_to_tree")
    parser.add_argument("nc_path", metavar="NETCDF_FILE")
    parser.add_argument("-o", "--out-dir", metavar="DIR", default=os.getcwd())
    parser.add_argument("--product-group", metavar="PRODUCT_GROUP",
                        help="e.g., meteosim", default='meteosim')
    parser.add_argument("--product-class", metavar="PRODUCT_CLASS",
                        help="e.g., moloch")
    parser.add_argument("--instance-uid", metavar="UID",
                        help="an unique identifier for this dataset")
    parser.add_argument("--url-root", metavar="URL_ROOT",
                        help="the url root of the data tree",
                        default="https://rest.tdm-project.it")
    parser.set_defaults(func=main)
