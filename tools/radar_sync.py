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
Performs a copy of meteorological images from an SFTP folder
to an HDFS one. The path for both the source and destination folders are
compliant with the TDM folder convention for meteorological images:

    <ROOT_DIR>/YYYY/MM/DD/

Files are copied to HDFS only if the destination folder does not exist or the
file count differs from that of the source folder. No hash or size check is
performed.
"""

# pylint: disable=import-error, broad-except
# pylint: disable=too-many-arguments


import io
import os
import re
import sys
import stat
import logging
import argparse
import traceback

import paramiko
from pydoop import hdfs


class SSHClient():
    """
    Class that handles ssh/sftp remote connections and operations.
    """
    def __init__(self,
                 username,
                 hostname,
                 port=22,
                 key_file=None,
                 root_dir='/'):
        self._hostname = hostname
        self._port = port
        self._username = username
        self._key_file = key_file
        self._client = paramiko.SSHClient()
        self._root_dir = root_dir

    def list_folder(self, year='', month='', day=''):
        """
        Lists folder contents.
        """
        try:
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                hostname=self._hostname,
                port=self._port,
                username=self._username,
                key_filename=self._key_file
            )

            _folder = '{}/{}/{}/{}'.format(self._root_dir, year, month, day)

            _, stdout, _ = self._client.exec_command('ls {}'.format(_folder))
            _contents = []

            for line in stdout:
                _contents.append(line.strip('\n'))

            self._client.close()
        except Exception as ex:
            logging.critical("*** Caught exception: %s: %s", ex.__class__, ex)
            try:
                self._client.close()
            except Exception:
                pass

        return _contents

    def get_folder_size(self, year='', month='', day=''):
        """
        Gets folder size in MB.
        """
        _content = 0

        try:
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._client.connect(
                hostname=self._hostname,
                port=self._port,
                username=self._username,
                key_filename=self._key_file
            )

            _folder = '{}/{}/{}/{}'.format(self._root_dir, year, month, day)

            _, stdout, _ = self._client.exec_command(
                'du -ms {}'.format(_folder))

            for line in stdout:
                _content, _ = line.strip('\n').split()

            self._client.close()
        except Exception as ex:
            logging.critical("*** Caught exception: %s: %s", ex.__class__, ex)
            try:
                self._client.close()
            except Exception:
                pass

        return int(_content)

    def remote_to_local_copy(self, folder):
        """
        Copies the contents of a remote folder."
        """
        _transport = paramiko.Transport((self._hostname, self._port))
        _r_key = paramiko.RSAKey.from_private_key_file(self._key_file)

        try:
            _transport.connect(
                username=self._username,
                pkey=_r_key
            )

            _client = paramiko.SFTPClient.from_transport(_transport)
            _remote_path = '{}/{}'.format(self._root_dir, folder)
            _local_path = os.path.basename(_remote_path)
            _client.chdir(_remote_path)

            if not os.path.isdir(_local_path):
                os.mkdir(_local_path)

            for _file in _client.listdir("."):
                if stat.S_ISREG(_client.stat(_file).st_mode):
                    logging.debug("Retrieving SFTP remote file '%s'", _file)
                    _client.get(_file, f'{_local_path}/{_file}')

        except Exception as ex:
            logging.critical("*** Caught exception: %s: %s", ex.__class__, ex)
            traceback.print_exc()
            _transport.close()
            sys.exit(1)

        _transport.close()


def check_hdfs_url(hdfs_url):
    """
    Checks if the given string represents a valid HDFS path.
    """
    _match = re.match(
        r'((?P<schema>\w+)://)?'
        r'(?P<host>[\w\._-]+)'
        r'(:(?P<port>\d+))?'
        r'(/(?P<path>[a-zA-Z0-9/]+))?',
        hdfs_url)

    if _match:
        if _match.group('schema') and _match.group('schema') != 'hdfs':
            return None

        if _match.group('port'):
            _url = (f'hdfs://{_match.group("host")}:{_match.group("port")}/'
                    f'{_match.group("path")}')
        else:
            _url = f'hdfs://{_match.group("host")}/{_match.group("path")}'

        return _url

    return None


def check_ssh_url(ssh_url):
    """
    Checks if the given string represents a valid SSH/SFTP path.
    """
    _match = re.match(
        r'((?P<schema>\w+)://)?'
        r'((?P<user>[\w\._-]+)@)?'
        r'(?P<host>[\w\._-]+)'
        r'(:(?P<port>\d*))?'
        r'(?P<path>/[\w\._\-/]*)?',
        ssh_url)

    if _match:
        if _match.group('schema') and _match.group('schema') != 'ssh':
            return (None, None, None, None)

        return (_match.group('user'), _match.group('host'),
                int(_match.group('port')), _match.group('path'))

    return (None, None, None, None)


def copy_to_hdfs(local_folder, remote_folder):
    """
    Performs a copy of the local files to the HDFS remote folder.
    """
    _host, _port, _out_dir = hdfs.path.split(remote_folder)
    _fs = hdfs.hdfs(_host, _port)

    if not _fs.exists(_out_dir):
        _fs.create_directory(_out_dir)

    for _entry in os.scandir(local_folder):
        if _entry.is_dir():
            continue

        _local_file = _entry.path
        _remote_file = os.path.join(_out_dir, _entry.name)

        with io.open(_local_file, "rb") as _fi:
            with _fs.open_file(_remote_file, "wb") as _fo:
                _fo.write(_fi.read())


def list_hdfs_files(hdfs_url):
    """
    Scans the remote HDFS filesystem and returns a list of folders present
    in the given path and the number of files inside.
    """
    _hdfs_remote_files = {}

    for _hdfs_year in [hdfs.path.basename(_) for _ in hdfs.ls(hdfs_url)]:
        _hdfs_path = f"{_hdfs_year}"
        for _hdfs_month in [hdfs.path.basename(_)
                            for _ in hdfs.ls(f'{hdfs_url}/{_hdfs_path}')]:
            _hdfs_path = f"{_hdfs_year}/{_hdfs_month}"
            for _hdfs_day in [hdfs.path.basename(_)
                              for _ in hdfs.ls(f'{hdfs_url}/{_hdfs_path}')]:
                _hdfs_path = (f"{_hdfs_year}/{_hdfs_month}/{_hdfs_day}")
                _hdfs_files = hdfs.ls(f'{hdfs_url}/{_hdfs_path}')
                _hdfs_remote_files[_hdfs_path] = len(_hdfs_files)
                logging.debug("Found HDFS path %s, %d files",
                              _hdfs_path, len(_hdfs_files))

    return _hdfs_remote_files


def ssh_hdfs_folder_diff(ssh_client, hdfs_tree):
    """
    Scans the remote SFTP repository and returns a list of folder non present
    in the HDFS filesystem.
    """
    _ssh_days_to_retrieve = []

    _ssh_years = ssh_client.list_folder()
    for _ssh_year in _ssh_years:
        _ssh_months = ssh_client.list_folder(_ssh_year)
        for _ssh_month in _ssh_months:
            _ssh_days = ssh_client.list_folder(_ssh_year, _ssh_month)
            for _ssh_day in _ssh_days:
                _ssh_files = ssh_client.list_folder(
                    _ssh_year, _ssh_month, _ssh_day)
                _ssh_file_count = len(_ssh_files)
                _ssh_path = f'{_ssh_year}/{_ssh_month}/{_ssh_day}'

                if _ssh_path in hdfs_tree:
                    if _ssh_file_count != hdfs_tree[_ssh_path]:
                        logging.debug(
                            "SSH path '%s' found in HDFS but file count"
                            " differs %d != %d", _ssh_path, _ssh_file_count,
                            hdfs_tree[_ssh_path])
                        _ssh_days_to_retrieve.append(_ssh_path)
                    else:
                        logging.debug(
                            "SSH path '%s' found in HDFS: skipped", _ssh_path)
                else:
                    if _ssh_file_count > 0:
                        logging.debug(
                            "SSH path '%s' appended, %d files found",
                            _ssh_path, _ssh_file_count)
                        _ssh_days_to_retrieve.append(_ssh_path)
                    else:
                        logging.debug(
                            "SSH '%s' path is empty, skipped",
                            _ssh_path)

    return _ssh_days_to_retrieve


def delete_local_folder(local_folder, dryrun):
    """
    Deletes the local folder used as temporary container for the remote SSH
    files.
    """
    logging.info("Deleting local folder '%s'", local_folder)
    if not dryrun:
        for _root, _dirs, _files in os.walk(local_folder, topdown=False):
            for _file in _files:
                os.remove(os.path.join(_root, _file))
            for _file in _dirs:
                os.rmdir(os.path.join(_root, _file))
            os.rmdir(local_folder)


def main():
    """
    Parses the command line and performs the SSH-to-HDFS copy.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', '-n', action='store_true',
                        help=('check HDFS/SSH files diffrerences only, '
                              'does not perform any copy'))
    parser.add_argument('--debug', '-d', action='store_true',
                        help=('prints debug messages'))
    parser.add_argument('--hdfs', action='store', type=str, required=True,
                        dest='hdfs_url',
                        help=(
                            'hdfs server and path of the form: '
                            'hdfs://<NAME_NODE>:<PORT>/PATH'))
    parser.add_argument('--ssh', action='store', type=str, required=True,
                        dest='ssh_url',
                        help=(
                            'ssh server and path of the form: '
                            '<USER>@<NAME_NODE>:<PORT>/PATH'))
    parser.add_argument('--key', action='store', type=str, required=True,
                        dest='ssh_key',
                        help=('key for ssh server authentication'))

    args = parser.parse_args()

    # If the debug flag is set, print all messages
    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='[%(levelname)s] %(message)s')
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='[%(levelname)s] %(message)s')

    logging.getLogger("paramiko").setLevel(logging.WARNING)

    _hdfs_url = check_hdfs_url(args.hdfs_url)
    if _hdfs_url is None:
        logging.error(
            'Wrong, incomplete or absent HDFS path: \'%s\'', args.hdfs_url)
        sys.exit(1)

    (_ssh_username, _ssh_hostname, _ssh_port,
     _ssh_root) = check_ssh_url(args.ssh_url)
    if _ssh_hostname is None:
        logging.error(
            'Wrong, incomplete or absent SSH path: \'%s\'', args.ssh_url)
        sys.exit(1)

    _hdfs_remote_files = list_hdfs_files(_hdfs_url)

    ssh_client = SSHClient(
        username=_ssh_username,
        hostname=_ssh_hostname,
        port=_ssh_port,
        key_file=args.ssh_key,
        root_dir=_ssh_root
    )

    _ssh_days_to_retrieve = ssh_hdfs_folder_diff(
        ssh_client,
        _hdfs_remote_files)

    logging.info('There are %d folders to copy from SFTP to HDFS',
                 len(_ssh_days_to_retrieve))

    for _ssh_day_to_retrieve in _ssh_days_to_retrieve:
        _ssh_files = ssh_client.list_folder(_ssh_day_to_retrieve)
        _local_path = os.path.basename(_ssh_day_to_retrieve).lstrip('/')
        _hdfs_path = f'{_hdfs_url}/{_ssh_day_to_retrieve}'

        logging.info(
            "Retrieving SFTP folder '%s', %d files, %dM to local folder '%s'",
            _ssh_day_to_retrieve, len(_ssh_files),
            ssh_client.get_folder_size(_ssh_day_to_retrieve),
            _local_path)

        if not args.dryrun:
            ssh_client.remote_to_local_copy(_ssh_day_to_retrieve)

        logging.info(
            "Copying local folder '%s' to HDFS folder '%s'",
            _local_path, _hdfs_path)
        if not args.dryrun:
            copy_to_hdfs(_local_path, _hdfs_path)

        delete_local_folder(_local_path, args.dryrun)


if __name__ == "__main__":
    main()
