IGNITE_DEFAULT_HOST = 'localhost'
IGNITE_DEFAULT_PORT = 10800


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


def pytest_generate_tests(metafunc):
    if 'ignite_host' in metafunc.fixturenames:
        metafunc.parametrize(
            'ignite_host',
            metafunc.config.getoption('ignite_host') or [IGNITE_DEFAULT_HOST],
            scope='session'
        )
    if 'ignite_port' in metafunc.fixturenames:
        metafunc.parametrize(
            'ignite_port',
            metafunc.config.getoption('ignite_port') or [IGNITE_DEFAULT_PORT],
            scope = 'session'
        )
