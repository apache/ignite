# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from distutils.util import strtobool
import ssl

import pytest

from pyignite import Client
from pyignite.constants import *
from pyignite.api import cache_create, cache_get_names, cache_destroy


class UseSSLParser(argparse.Action):

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
    ignite_host, ignite_port, timeout, use_ssl, ssl_keyfile, ssl_keyfile_password, ssl_certfile,
    ssl_ca_certfile, ssl_cert_reqs, ssl_ciphers, ssl_version,
    username, password,
):
    client = Client(
        timeout=timeout,
        use_ssl=use_ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_keyfile_password=ssl_keyfile_password,
        ssl_certfile=ssl_certfile,
        ssl_ca_certfile=ssl_ca_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ciphers=ssl_ciphers,
        ssl_version=ssl_version,
        username=username,
        password=password,
    )
    client.connect(ignite_host, ignite_port)
    yield client
    for cache_name in cache_get_names(client).value:
        cache_destroy(client, cache_name)
    client.close()


@pytest.fixture
def cache(client):
    cache_name = 'my_bucket'
    cache_create(client, cache_name)
    yield cache_name
    cache_destroy(client, cache_name)


def pytest_addoption(parser):
    parser.addoption(
        '--ignite-host',
        action='append',
        default=[IGNITE_DEFAULT_HOST],
        help='Ignite binary protocol test server host (default: localhost)'
    )
    parser.addoption(
        '--ignite-port',
        action='append',
        default=[IGNITE_DEFAULT_PORT],
        type=int,
        help='Ignite binary protocol test server port (default: 10800)'
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
        '--use-ssl',
        action=UseSSLParser,
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
        '--ssl-keyfile-password',
        action='store',
        default=None,
        type=str,
        help='password for SSL key file'
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
        'ignite_host': IGNITE_DEFAULT_HOST,
        'ignite_port': IGNITE_DEFAULT_PORT,
        'timeout': None,
        'use_ssl': False,
        'ssl_keyfile': None,
        'ssl_keyfile_password': None,
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
            if type(param) is not list:
                param = [param]
            metafunc.parametrize(param_name, param, scope='session')
