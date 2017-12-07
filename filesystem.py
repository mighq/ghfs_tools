#!/usr/bin/env python

# based on github.com/skorokithakis/python-fuse-sample

import argparse
import array
import errno
import hashlib
import logging
import math
import os
import struct
import sys

import fuse
import yaml

class CombineFs(fuse.Operations):
    def __init__(self, config):
        self.config = config

        hasher = hashlib.new(self.config['hash'])
        self.hash_size = hasher.digest_size

    # Helpers
    # =======

    def _hash_path(self, partial):
        hasher = hashlib.new(self.config['hash'])
        hasher.update(partial)
        digest = hasher.hexdigest()

        logging.debug('hashed %s to %s', partial, digest)

        return digest

    def _path_in_shard_range(self, path, range_cfg):
        obj_hash = self._hash_path(path)
        hash_int = long(obj_hash, 16)

        ldelim = long(str(range_cfg[0]).ljust(self.hash_size * 2, str(range_cfg[0])[-1]), 16)
        if hash_int < ldelim:
            return False

        rdelim = long(str(range_cfg[1]).ljust(self.hash_size * 2, str(range_cfg[1])[-1]), 16)
        if hash_int > rdelim:
            return False

        return True

    def _sharded_path(self, partial):
        shard_prefix = None

        for shard_cfg in self.config['shards']:
            if self._path_in_shard_range(partial, shard_cfg['range']):
                logging.debug('choosing shard %s', str(shard_cfg))
                shard_prefix = shard_cfg['path']

        if shard_prefix is None:
            raise Exception('shard not found for the hash', obj_hash)

        return shard_prefix + partial

    def _full_path(self, partial):
        if not partial.startswith("/"):
            partial = '/' + partial

        path = self._sharded_path(partial)

        logging.debug('translated path %s to %s', partial, path)

        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        logging.info('access')
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise fuse.FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        logging.info('chmod')
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        logging.info('chown')
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        logging.info('getattr')

        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        logging.info('readdir')

        dirents = ['.', '..']

        for shard_cfg in self.config['shards']:
            full_path = shard_cfg['path'] + path

            if os.path.isdir(full_path):
                filelist = os.listdir(full_path)
                dirents.extend(filelist)

        dirents = list(set(dirents))

        logging.debug('directory entries: %s', str(dirents))

        for r in dirents:
            # hide /.git folders from the raw ones
            # meaning, we cannot have .git in the mount too
            if path == '/' and r == '.git':
                continue

            yield r

    def readlink(self, path):
        logging.info('readlink')

        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        logging.info('mknod')
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        logging.info('rmdir')

        for shard_cfg in self.config['shards']:
            full_path = shard_cfg['path'] + '/' + path
            os.rmdir(full_path)

    def mkdir(self, path, mode):
        logging.info('mkdir')

        for shard_cfg in self.config['shards']:
            full_path = shard_cfg['path'] + '/' + path
            os.mkdir(full_path, mode)

    def statfs(self, path):
        logging.info('statfs')

        block = 4096

        total_size = 0
        for shard_cfg in self.config['shards']:
            total_size += shard_cfg['capacity']

        stv = {
          'f_bsize'  : block,
          'f_blocks' : int(math.floor(total_size / block)),
          'f_bfree'  : 0,
        }

        logging.debug(stv)

        return stv

    def unlink(self, path):
        logging.info('unlink')
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        logging.info('symlink')
        return os.symlink(target, self._full_path(name))

    def rename(self, old, new):
        logging.info('rename')
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        logging.info('link')
        return os.link(self._full_path(name), self._full_path(target))

    def utimens(self, path, times=None):
        logging.info('utimens')
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        logging.info('open')
        full_path = self._full_path(path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        logging.info('create')
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        logging.info('read')
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        logging.info('write')
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        logging.info('truncate')
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        logging.info('flush')
        return os.fsync(fh)

    def release(self, path, fh):
        logging.info('release')
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        logging.info('fsync')
        return self.flush(path, fh)


def main():
    global log

    cli = argparse.ArgumentParser(description='Process some integers.')
    cli.add_argument('-c', '--config', dest='config', required=True, help='config file')
    cli.add_argument('-m', '--mount',  dest='mount',  required=True, help='mount point path')
    cli_params = cli.parse_args()

    if not os.path.isfile(cli_params.config):
        raise Exception('invalid config file', cli_params.config)

    with open(cli_params.config, 'r') as fp:
        config = yaml.load(fp)

    log = logging.getLogger()
    log.setLevel(getattr(logging, config['log_level'].upper()))
    stderr_log_handler = logging.StreamHandler(sys.stderr)
    stderr_log_handler.setFormatter(logging.Formatter(datefmt='%Y-%m-%d %H:%M:%S', fmt='%(asctime)s [%(levelname)s] %(message)s'))
    log.addHandler(stderr_log_handler)

    fuse.FUSE(CombineFs(config), cli_params.mount, nothreads=True, foreground=True)

if __name__ == '__main__':
    main()
