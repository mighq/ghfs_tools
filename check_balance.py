#!/usr/bin/env python

import argparse
import hashlib
import logging
import os
import sys

import yaml

class Checker(object):
    def __init__(self, config):
        self.config = config

        hasher = hashlib.new(self.config['hash'])
        self.hash_size = hasher.digest_size

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

    def check(self):
        for shard_cfg in self.config['shards']:
            for i_root, i_dir, i_files in os.walk(shard_cfg['path']):
                rel_path = i_root[len(shard_cfg['path']):]

                # skip .git folders in the raws
                if rel_path == '/.git' or rel_path.startswith('/.git/'):
                    continue

                for f in i_files:
                    rel_with_file = rel_path + '/' + f
                    if not self._path_in_shard_range(rel_with_file, shard_cfg['range']):
                        loging.warning('file "%s" not in expected shard', rel_with_file)

def main():
    cli = argparse.ArgumentParser(description='Process some integers.')
    cli.add_argument('-c', '--config', dest='config', required=True, help='config file')
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

    obj = Checker(config)
    obj.check()

if __name__ == '__main__':
    main()
