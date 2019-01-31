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

"""\
Copy radar images to HDFS, converting file names to avoid illegal characters.
"""

import argparse
from datetime import datetime
import io
import os
import sys

import pydoop.hdfs as hdfs

join = os.path.join
splitext = os.path.splitext
strptime = datetime.strptime

OUT_FMT = "%Y%m%d%H%M%S"

# from tdm.radar.utils import get_images

FMT = "%Y-%m-%d_%H:%M:%S"
FMT_LEN = 4 + 5 * 3  # %Y is 4 chars, other fields are 2 chars
MIN_DT, MAX_DT = datetime.min, datetime.max


def get_images(root, after=MIN_DT, before=MAX_DT):
    ls = []
    for entry in os.scandir(root):
        if entry.is_dir():
            continue
        dt_string = splitext(entry.name)[0][-FMT_LEN:]
        try:
            dt = strptime(dt_string, FMT)
        except ValueError:
            continue
        if dt < after or dt > before:
            continue
        ls.append((dt, entry.path))
    ls.sort()
    return ls


def main(args):
    host, port, out_dir = hdfs.path.split(args.out_dir)
    fs = hdfs.hdfs(host, port)
    fs.create_directory(out_dir)
    for dt, path in get_images(args.in_dir):
        out_path = join(out_dir, f"{dt.strftime(OUT_FMT)}.png")
        with io.open(path, "rb") as fi:
            with fs.open_file(out_path, "wb") as fo:
                fo.write(fi.read())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("in_dir", metavar="INPUT_DIR")
    parser.add_argument("out_dir", metavar="OUTPUT_DIR")
    main(parser.parse_args(sys.argv[1:]))
