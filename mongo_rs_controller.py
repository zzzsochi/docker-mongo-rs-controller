#!/usr/bin/env python3

import argparse
import contextlib
import itertools
import logging
import os
import socket
import sys
import time
from typing import Optional, Union, Tuple, List, Dict

import pymongo


logger = logging.getLogger(name='mongo_controller')


class InstancesNotStarted(Exception):
    pass


@contextlib.contextmanager
def connection(addr: str) -> pymongo.MongoClient:
    conn = pymongo.MongoClient('mongodb://{}/'.format(addr))
    yield conn
    conn.close()


def run_command(addr: str, command: str, value: Optional[dict] = None) -> dict:
    logger.debug(f"run_command({addr!r}, {command!r}, {value!r})")
    with connection(addr) as mongo:
        result = mongo['admin'].command(command, value)
        logger.debug(f"run_command({addr!r}, {command!r}, {value!r}) -> {result!r}")
        return result


def get_instances(hostnames: List[str]) -> Tuple[Optional[str], List[str], List[str]]:
    addresses = set()

    for hostname in hostnames:
        try:
            addr_info = socket.getaddrinfo(hostname, 0, 0, 0, 0)
        except socket.gaierror:
            logger.info(f"{hostname!r} does not resolve")
        else:
            for family, type, proto, canonname, sockaddr in addr_info:
                if (family in [socket.AF_INET, socket.AF_INET6] and
                        type == socket.SOCK_STREAM):
                    addresses.add(sockaddr[0])

    primary = None
    secondaries = set()
    vacants = set()

    for addr in addresses:
        try:
            instance_info = run_command(addr, 'isMaster')
        except pymongo.errors.ServerSelectionTimeoutError:
            logger.info(f"mongodb on {addr!r} not answered")
        else:
            if instance_info['ismaster']:
                if primary is None:
                    primary = addr
                else:
                    raise RuntimeError(f"split head detected! {primary} {addr}")

            elif instance_info['secondary']:
                secondaries.add(addr)

            else:
                vacants.add(addr)

    return primary, sorted(secondaries), sorted(vacants)


def init_rs(hostnames: List[str]):
    primary, secondaries, vacants = get_instances(hostnames)

    if primary:
        logger.info(f'primary found: {primary}')
        return

    elif not primary and secondaries:
        logger.info(f'primary not found but secondaries')
        raise InstancesNotStarted()

    elif not primary and not secondaries and vacants:
        primary = vacants[0]
        logger.info(f'create new primary: {primary}')
        config = {'_id': 'rs',
                  'version' : 1,
                  'members': [{'_id': 0, 'host': primary}]}

        run_command(primary, 'replSetInitiate', config)
        return

    else:
        logger.info(f'instances not started')
        raise InstancesNotStarted()


def reconfigure(primary: str, members: List[Dict], removed: List[Dict]):
    logger.info(f'reconfigure primary: {primary}')
    logger.debug(f'reconfigure({primary!r}, {members!r}, {removed!r})')

    members.sort(key=lambda m: m['_id'])

    current_config = run_command(primary, 'replSetGetConfig')['config']
    version = current_config['version'] + 1
    logger.debug(f'new version: {version}')

    logger.info(f'reconfigure with {len(members)} members')
    logger.debug(f'members {[r["host"] for r in members]!r}')

    logger.info(f'removed {len(removed)} members')
    logger.debug(f'removed {[r["name"] for r in removed]!r}')

    config = {'_id': 'rs', 'version': version, 'members': members}
    run_command(primary, 'replSetReconfig', config)


def watch(hostnames: List[str]):
    while True:
        time.sleep(5)

        primary, secondaries, vacants = get_instances(hostnames)
        if not primary:
            logger.warning(f'primary not found')
            continue

        if vacants:
            logger.info(f'vacants found: {vacants!r}')

            members = []
            removed = []

            for member in run_command(primary, 'replSetGetStatus')['members']:
                if member['state'] == 8:
                    removed.append(member)
                else:
                    members.append({'_id': member['_id'], 'host': member['name']})

            last_id = max(m['_id'] for m in members)

            addresses = sorted(vacants)
            for n, addr in enumerate(addresses, last_id + 1):
                members.append({'_id': n, 'host': addr})

            reconfigure(primary, members, removed)

        else:
            members = []
            removed = []

            for member in run_command(primary, 'replSetGetStatus')['members']:
                if member['state'] == 8:
                    removed.append(member)
                else:
                    members.append({'_id': member['_id'], 'host': member['name']})

            if removed:
                reconfigure(primary, members, removed)


def argparser(argv):
    parser = argparse.ArgumentParser(
        prog=os.path.basename(argv[0]),
        description="Setup replica set for MongoDB.",
    )
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('hostnames', nargs='+')

    return parser.parse_args(argv[1:])


def main():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        level=os.environ.get('LOGLEVEL', 'INFO'))

    args = argparser(sys.argv)

    initialized = False
    while not initialized:
        try:
            init_rs(args.hostnames)
            initialized = True
        except InstancesNotStarted:
            time.sleep(5)

    if args.watch:
        watch(args.hostnames)


if __name__ == '__main__':
    main()
