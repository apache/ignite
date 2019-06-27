#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import argparse
from distutils.util import strtobool
import ssl

import pytest

from pygridgain import Client
from pygridgain.constants import *
from pygridgain.api import cache_create, cache_get_names, cache_destroy


class BoolParser(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        values = True if values is None else bool(strtobool(values))
        setattr(namespace, self.dest, values)


class CertReqsParser(argparse.Action):
    conv_map = {
        'NONE': ssl.CERT_NONE,
        'OPTIONAL': ssl.CERT_OPTIONAL,
        'REQUIRED': ssl.CERT_REQUIRED,
    }

    def __call__(self, parser, namespace, values, option_string=None):
        value = values.upper()
        if value in self.conv_map:
            setattr(namespace, self.dest, self.conv_map[value])
        else:
            raise ValueError(
                'Undefined argument: --ssl-cert-reqs={}'.format(value)
            )


class SSLVersionParser(argparse.Action):
    conv_map = {
        'TLSV1_1': ssl.PROTOCOL_TLSv1_1,
        'TLSV1_2': ssl.PROTOCOL_TLSv1_2,
    }

    def __call__(self, parser, namespace, values, option_string=None):
        value = values.upper()
        if value in self.conv_map:
            setattr(namespace, self.dest, self.conv_map[value])
        else:
            raise ValueError(
                'Undefined argument: --ssl-version={}'.format(value)
            )


@pytest.fixture(scope='module')
def client(
    node, timeout, affinity_aware, use_ssl, ssl_keyfile, ssl_certfile,
    ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version,
    username, password,
):
    client = Client(
        timeout=timeout,
        affinity_aware=affinity_aware,
        use_ssl=use_ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_ca_certfile=ssl_ca_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ciphers=ssl_ciphers,
        ssl_version=ssl_version,
        username=username,
        password=password,
    )
    nodes = []
    for n in node:
        host, port = n.split(':')
        port = int(port)
        nodes.append((host, port))
    client.connect(nodes)
    yield client
    conn = client.random_node
    for cache_name in cache_get_names(conn).value:
        cache_destroy(conn, cache_name)
    client.close()


@pytest.fixture
def cache(client):
    cache_name = 'my_bucket'
    conn = client.random_node

    cache_create(conn, cache_name)
    yield cache_name
    cache_destroy(conn, cache_name)


def pytest_addoption(parser):
    parser.addoption(
        '--node',
        action='append',
        default=None,
        help=(
            'GridGain binary protocol test server connection string '
            '(default: "localhost:10800")'
        )
    )
    parser.addoption(
        '--timeout',
        action='store',
        type=float,
        default=None,
        help=(
            'Timeout (in seconds) for each socket operation. Can accept '
            'integer or float value. Default is None'
        )
    )
    parser.addoption(
        '--affinity-aware',
        action=BoolParser,
        nargs='?',
        default=False,
        help='Turn on the best effort affinity feature'
    )
    parser.addoption(
        '--use-ssl',
        action=BoolParser,
        nargs='?',
        default=False,
        help='Use SSL encryption'
    )
    parser.addoption(
        '--ssl-keyfile',
        action='store',
        default=None,
        type=str,
        help='a path to SSL key file to identify local party'
    )
    parser.addoption(
        '--ssl-certfile',
        action='store',
        default=None,
        type=str,
        help='a path to ssl certificate file to identify local party'
    )
    parser.addoption(
        '--ssl-ca-certfile',
        action='store',
        default=None,
        type=str,
        help='a path to a trusted certificate or a certificate chain'
    )
    parser.addoption(
        '--ssl-cert-reqs',
        action=CertReqsParser,
        default=ssl.CERT_NONE,
        help=(
            'determines how the remote side certificate is treated: '
            'NONE (ignore, default), '
            'OPTIONAL (validate, if provided) or '
            'REQUIRED (valid remote certificate is required)'
        )
    )
    parser.addoption(
        '--ssl-ciphers',
        action='store',
        default=SSL_DEFAULT_CIPHERS,
        type=str,
        help='ciphers to use'
    )
    parser.addoption(
        '--ssl-version',
        action=SSLVersionParser,
        default=SSL_DEFAULT_VERSION,
        help='SSL version: TLSV1_1 or TLSV1_2'
    )
    parser.addoption(
        '--username',
        action='store',
        type=str,
        help='user name'
    )
    parser.addoption(
        '--password',
        action='store',
        type=str,
        help='password'
    )
    parser.addoption(
        '--examples',
        action='store_true',
        help='check if examples can be run',
    )


def pytest_generate_tests(metafunc):
    session_parameters = {
        'node': ['{host}:{port}'.format(host=DEFAULT_HOST, port=DEFAULT_PORT)],
        'timeout': None,
        'affinity_aware': False,
        'use_ssl': False,
        'ssl_keyfile': None,
        'ssl_certfile': None,
        'ssl_ca_certfile': None,
        'ssl_cert_reqs': ssl.CERT_NONE,
        'ssl_ciphers': SSL_DEFAULT_CIPHERS,
        'ssl_version': SSL_DEFAULT_VERSION,
        'username': None,
        'password': None,
    }

    for param_name in session_parameters:
        if param_name in metafunc.fixturenames:
            param = metafunc.config.getoption(param_name)
            if param is None:
                param = session_parameters[param_name]
            if param_name == 'node' or type(param) is not list:
                param = [param]
            metafunc.parametrize(param_name, param, scope='session')
