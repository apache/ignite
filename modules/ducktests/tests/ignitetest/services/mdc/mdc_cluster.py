# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MDC test fixture.

    mdc = MdcCluster(self, ignite_version, srv_per_dc=3, runners_per_dc=1)

    with cross_dc_network(self.logger, mdc, delay_ms=20) as net:
        mdc.start_servers()
        mdc.generate_data(DC_1, CACHE, 0, 1000)
        mdc.verify_cache_distribution(CACHE, copies_per_dc=1)

        net.enable_network_partition(DC_1, DC_2)
        ...

The fixture spans an arbitrary number of data centers, which selects the
:class:`MdcTopologyValidator` mode (see :func:`mdc_topology_params`):

    mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1)

    with cross_dc_network(self.logger, mdc, delay_ms=20) as net:
        net.enable_network_partitions(*isolation_pairs(DC_3, mdc.dcs))
        mdc.verify_segments((DC_1, DC_2), DC_3)

Globals:

    mdc_cache_topology_validator - whether the MDC caches are created with the cache level
    MdcTopologyValidator, default true.
"""
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Tuple, Union

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.network_group.manager import NetworkGroupManager
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration, TcpCommunicationSpi
from ignitetest.services.utils.ignite_configuration.discovery import TcpDiscoverySpi, from_ignite_cluster, \
    from_ignite_services
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils.version import IgniteVersion

DC_1 = "DC1"
DC_2 = "DC2"
DC_3 = "DC3"

# The two DC layouts the MDC topology validator distinguishes: an even DC count is
# validated against a main DC, an odd one by a majority of visible DCs.
DCS_2 = (DC_1, DC_2)
DCS_3 = (DC_1, DC_2, DC_3)

IGNITE_STARTUP_TIMEOUT_SEC = 90

# Global: set to false to create the MDC caches without the cache level topology validator.
CACHE_TOP_VALIDATOR_GLOBAL = "mdc_cache_topology_validator"

DATA_CENTER_ATTR = "IGNITE_DATA_CENTER_ID"
IGNITE_SQL_RETRY_TIMEOUT_ATTR = "IGNITE_SQL_RETRY_TIMEOUT"

IGNITE_SQL_RETRY_TIMEOUT_MS = 1_000

_APP_PKG = "org.apache.ignite.internal.ducktest.tests.mdc."

GENERATOR_APP = _APP_PKG + "MdcDataGeneratorApplication"
DATA_CHECKER_APP = _APP_PKG + "MdcDataCheckerApplication"
LOAD_APP = _APP_PKG + "MdcContinuousLoadApplication"
THIN_LOAD_APP = _APP_PKG + "MdcThinClientLoadApplication"

# Suspicious server log patterns: none of them is expected in any MDC scenario,
# partitioned or not. Matched against the node console capture.
LRT_PATTERN = "long running transactions"
PME_FREEZE_PATTERN = "Failed to wait for partition map exchange"
LOST_PARTITIONS_PATTERN = "Detected lost partitions"
ASSERTION_ERROR_PATTERN = "AssertionError"

# Every log of a server node, including the ones rotated by a restart.
ALL_LOGS_GLOB = "ignite*.log*"

# A segment of a partitioned cluster: one DC or a group of DCs that still see each other.
Segment = Union[str, Sequence[str]]


def dc_jvm_opts(dc: str) -> List[str]:
    """
    :return: JVM options assigning a node to the given data center.
    """
    return [f"-D{DATA_CENTER_ATTR}={dc}", f"-D{IGNITE_SQL_RETRY_TIMEOUT_ATTR}={IGNITE_SQL_RETRY_TIMEOUT_MS}"]


def mdc_topology_params(dcs: Sequence[str], main_dc: Optional[str] = None) -> dict:
    """
    Compiles the cache parameters that pin ``MdcTopologyValidator`` and
    ``MdcAffinityBackupFilter`` to the given DC set.

    The validator has two modes and the DC count picks one: with an EVEN number of DCs a
    segment stays writable while it sees the main DC (``mainDc``), with an ODD number
    while it sees a majority of the DC set (``datacenters``). Passing both is rejected by
    ``MdcTopologyValidator.checkConfiguration()``, so exactly one is emitted here.

    :param dcs: All data centers the cluster spans.
    :param main_dc: Main DC for the even-count mode, defaults to the first DC. Ignored for
           an odd DC count, where the validator is majority based.
    """
    params = {"dcsNum": len(dcs)}

    if len(dcs) % 2 == 1:
        params["datacenters"] = list(dcs)
    else:
        params["mainDc"] = main_dc if main_dc is not None else dcs[0]

    return params


def min_backups(dcs: Sequence[str]) -> int:
    """
    :return: Smallest backup count that gives every DC exactly one copy of every partition.
             ``MdcAffinityBackupFilter`` requires ``(backups + 1)`` to be divisible by the
             number of DCs, so this is the smallest admissible value at all.
    """
    return len(dcs) - 1


def all_pairs(dcs: Sequence[str]) -> List[Tuple[str, str]]:
    """
    :return: Every unordered DC pair - the full cross-DC mesh.
    """
    return list(combinations(dcs, 2))


def isolation_pairs(dc: str, dcs: Sequence[str]) -> List[Tuple[str, str]]:
    """
    :return: The DC pairs that cut ``dc`` off from every other DC, leaving the rest
             connected. Feed to :meth:`NetworkGroupManager.enable_network_partitions`.
    """
    return [(dc, other) for other in dcs if other != dc]


def _per_dc(value: Union[int, Dict[str, int]], dcs: Sequence[str]) -> Dict[str, int]:
    """
    Normalizes an int-or-dict per-DC count into a dict, e.g. 3 -> {DC1: 3, DC2: 3}.
    """
    return dict(value) if isinstance(value, dict) else {dc: value for dc in dcs}


def _as_segment(segment: Segment) -> Tuple[str, ...]:
    """
    Normalizes a single DC name or a collection of DC names into a tuple of DC names.
    """
    return (segment,) if isinstance(segment, str) else tuple(segment)


class MdcCluster:
    """
    Owns the per-DC Ignite services and reusable application services of an MDC test,
    plus the MDC-specific verification helpers.

    :param test: The ducktape test instance.
    :param ignite_version: Ignite version string.
    :param dcs: Data centers the cluster spans, two by default. The count selects the
           topology validator mode - see :func:`mdc_topology_params`.
    :param main_dc: Main DC for an even-sized DC set, defaults to the first DC.
    :param srv_per_dc: Servers per DC, an int or a per-DC dict (asymmetric DCs).
    :param runners_per_dc: Reusable run-to-completion app services per DC (generator,
           checkers, load bursts). An int or a per-DC dict.
    :param loaders_per_dc: Dedicated background load app services per DC. They run
           concurrently with runner apps, hence separate containers.
    :param client_connector: Whether to expose the thin client connector on servers.
    """
    def __init__(self, test, ignite_version: str, dcs: Sequence[str] = DCS_2,
                 main_dc: Optional[str] = None,
                 srv_per_dc: Union[int, Dict[str, int]] = 3,
                 runners_per_dc: Union[int, Dict[str, int]] = 1,
                 loaders_per_dc: Union[int, Dict[str, int]] = 0,
                 client_connector: bool = False,
                 network_timeout: int = 5_000,
                 tcp_connect_timeout: int = 5_000):
        self.test_context = test.test_context
        self.logger = test.logger

        self.dcs = tuple(dcs)

        assert len(self.dcs) >= 2, f"An MDC cluster spans at least two data centers [dcs={self.dcs}]"

        self.main_dc = main_dc if main_dc is not None else self.dcs[0]

        # A single discovery SPI (hence a single ip finder) shared by all DCs' server
        # services is what makes the DCs form ONE cluster: prepare_on_start() memoizes the
        # addresses of the first started DC into the shared ip finder, so every later DC
        # discovers through the first DC's nodes, and restart() re-joins the same way.
        # Restarting the first started DC itself is the one case this breaks - see
        # sync_service_discovery().
        cfg_kwargs = {
            "version": IgniteVersion(ignite_version),
            "discovery_spi": TcpDiscoverySpi(),
            "network_timeout": network_timeout,
            "communication_spi": TcpCommunicationSpi(connect_timeout=tcp_connect_timeout)
        }

        if client_connector:
            cfg_kwargs["client_connector_configuration"] = ClientConnectorConfiguration()

        self.ignite_config = IgniteConfiguration(**cfg_kwargs)

        self.srv_per_dc = _per_dc(srv_per_dc, self.dcs)

        self.servers: Dict[str, IgniteService] = {
            dc: IgniteService(self.test_context, self.ignite_config, num_nodes=num, jvm_opts=dc_jvm_opts(dc),
                              startup_timeout_sec=IGNITE_STARTUP_TIMEOUT_SEC)
            for dc, num in self.srv_per_dc.items() if num > 0}

        self.runners: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in _per_dc(runners_per_dc, self.dcs).items()}

        self.loaders: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in _per_dc(loaders_per_dc, self.dcs).items()}

        # Extra services (e.g. thin clients) registered into a DC's network group.
        self.extras: Dict[str, List] = {dc: [] for dc in self.dcs}

        # App services that have been started at least once: the first start is clean,
        # subsequent ones preserve work dirs (and logs - hence unique result prefixes).
        self._started_apps = set()

        # Admissibility checks run on reusable services, so each check needs a unique result prefix.
        self._adm_checks = 0

        # Cache parameters applied to every cache this fixture creates, unless a call overrides them.
        self.cache_defaults = {
            "topologyValidator": self.test_context.globals.get(CACHE_TOP_VALIDATOR_GLOBAL, True)
        }

        self.logger.info(f"MDC cache defaults [{self.cache_defaults}]")

    @property
    def min_backups(self) -> int:
        """
        :return: Smallest backup count giving every DC one copy of every partition
                 (2 for a three DC cluster, 1 for a two DC one).
        """
        return min_backups(self.dcs)

    def topology_params(self) -> dict:
        """
        :return: Cache parameters pinning the topology validator and the affinity backup
                 filter to this cluster's DC set.
        """
        return mdc_topology_params(self.dcs, self.main_dc)

    def sync_service_discovery(self):
        """
        Points every server service at a discovery SPI covering all DCs.

        Required before restarting the FIRST started DC: the shared ip finder holds only
        that DC's addresses, so after a full stop its nodes would seed off themselves and
        form a separate cluster instead of rejoining the surviving DCs.
        """
        discovery_spi = from_ignite_services(list(self.servers.values()))

        for service in self.servers.values():
            service.config = service.config._replace(discovery_spi=discovery_spi)

    def _app_service(self, dc: str) -> IgniteApplicationService:
        client_cfg = self.ignite_config._replace(client_mode=True, discovery_spi=from_ignite_cluster(self.servers[dc]))

        return IgniteApplicationService(self.test_context, client_cfg, jvm_opts=dc_jvm_opts(dc))

    def register(self, dc: str, service):
        """
        Registers an extra service (e.g. a thin client app) into a DC's network group,
        so netem impairments and partitions apply to it. Must be called before
        :func:`cross_dc_network` snapshots the registry into a :class:`NetworkGroupManager`.
        """
        self.extras[dc].append(service)

    def network_registry(self) -> Dict[str, List]:
        """
        :return: Network group registry: DC name -> all services belonging to that DC.
        """
        registry = {}

        for dc in self.dcs:
            services = []

            if dc in self.servers:
                services.append(self.servers[dc])

            services += self.runners.get(dc, [])
            services += self.loaders.get(dc, [])
            services += self.extras.get(dc, [])

            if services:
                registry[dc] = services

        return registry

    def thin_client_addresses(self) -> List[str]:
        """
        :return: Thin client addresses of all server nodes across all DCs.
        """
        port = self.ignite_config.client_connector_configuration.port

        return [f"{node.account.hostname}:{port}"
                for dc in sorted(self.servers) for node in self.servers[dc].nodes]

    def start_servers(self):
        """
        Starts all server services.
        """
        for dc in sorted(self.servers):
            self.servers[dc].start()

    def stop_servers(self):
        """
        Stops all server services.
        """
        for dc in sorted(self.servers):
            self.servers[dc].stop()

    def restart(self, dc: str, clean: bool = False, await_rebalance: bool = True):
        """
        Restarts a whole DC preserving its persistence (the pattern used to rejoin a
        read-only segment back into the main cluster after a partition heals).
        """
        self.servers[dc].stop()
        self.servers[dc].start(clean=clean)

        if await_rebalance:
            self.servers[dc].await_rebalance()

    def run_app(self, dc: str, java_class: str, params: dict, runner: int = 0) -> IgniteApplicationService:
        """
        Runs a run-to-completion application on one of the DC's reusable runner services
        and returns the service (for ``extract_result``).
        """
        return self.run_service(self.runners[dc][runner], params, java_class=java_class)

    def run_service(self, svc: IgniteApplicationService, params: dict,
                    java_class: str = None) -> IgniteApplicationService:
        """
        Runs any reusable run-to-completion application service (a runner, a registered
        thin client, ...): the first start is clean, subsequent starts preserve work dirs.
        Returns the service (for ``extract_result``).
        """
        if java_class is not None:
            svc.java_class_name = java_class

        svc.params = self._with_topology_params(params)

        svc.start(clean=self._first_start(svc))
        svc.wait()
        svc.stop()

        return svc

    def start_loader(self, dc: str, params: dict, loader: int = 0,
                     java_class: str = LOAD_APP) -> IgniteApplicationService:
        """
        Starts a background load application (runs until stopped). Any exception raised
        by the application surfaces in :meth:`stop_loader`. :attr:`cache_defaults` are
        merged in.
        """
        svc = self.loaders[dc][loader]

        svc.java_class_name = java_class
        svc.params = self._with_topology_params({**self.cache_defaults, **params})

        svc.start(clean=self._first_start(svc))

        return svc

    def stop_loader(self, dc: str, loader: int = 0) -> IgniteApplicationService:
        """
        Stops a background load application. The application finishes its loop, records
        results and exits; a failed application fails the test here.
        """
        svc = self.loaders[dc][loader]

        svc.stop()

        return svc

    def _with_topology_params(self, params: dict) -> dict:
        """
        Injects this cluster's topology parameters into the parameters of an application
        that creates the MDC cache, so no call site can configure a validator or a backup
        filter that disagrees with the DC set. Explicit parameters still win.
        """
        if not params.get("createCache"):
            return params

        return {**self.topology_params(), **params}

    def _first_start(self, svc) -> bool:
        first = id(svc) not in self._started_apps

        self._started_apps.add(id(svc))

        return first

    def generate_data(self, dc: str, cache_name: str, from_idx: int, to_idx: int, backups: Optional[int] = None,
                      sql_mode: bool = False, **cache_params) -> IgniteApplicationService:
        """
        Creates the MDC cache (if absent) and populates keys ``[from_idx, to_idx)``.
        Extra cache parameters (``atomicity``, ``writeSync``, ``readFromBackup``,
        ``partitions``, ...) are passed through to the cache configuration builder, on top
        of :attr:`cache_defaults`.

        :param backups: Backup count, by default the smallest one that gives every DC a
               single copy of every partition (see :attr:`min_backups`).
        """
        params = {**self.topology_params(),
                  "cacheName": cache_name,
                  "backups": self.min_backups if backups is None else backups,
                  "from": from_idx, "to": to_idx, "sqlMode": sql_mode,
                  **self.cache_defaults, **cache_params}

        return self.run_app(dc, GENERATOR_APP, params)

    def check_data(self, dc: str, cache_name: str, from_idx: int, to_idx: int) -> Optional[IgniteApplicationService]:
        """
        Verifies that every key in ``[from_idx, to_idx)`` is readable and holds the
        expected value, from a client in the given DC.

        :return: The service that ran the check, or None for an empty range.
        """
        if to_idx <= from_idx:
            self.logger.debug(f"Nothing to check [cache={cache_name}, from={from_idx}, to={to_idx}]")
            return None

        params = {"cacheName": cache_name, "from": from_idx, "to": to_idx}

        return self.run_app(dc, DATA_CHECKER_APP, params)

    def check_put_admissibility(self, dc: str, cache_name: str, admissible: bool,
                                key_offset: int = 1_000_000, probes: int = 100) -> IgniteApplicationService:
        """
        Verifies that put load from the given DC is admissible (the segment passes the
        topology validator) or rejected by it (read-only segment). A PUT burst of the load
        application: an admissible check fails fast on the first rejected put, an
        inadmissible check fails if any of the probe puts succeeds.

        Probe keys start at ``key_offset`` (defaults to 1_000_000) so they never intersect
        with the data set verified by ``check_data``.
        """
        self._adm_checks += 1

        return self.run_load(dc, "PUT", cache_name, f"admCheck{self._adm_checks}",
                             keyFrom=key_offset, keyTo=key_offset + probes,
                             iterations=probes, inadmissible=not admissible)

    def run_load(self, dc: str, mode: str, cache_name: str, result_prefix: str,
                 runner: int = 0, **params) -> IgniteApplicationService:
        """
        Runs a load burst (see ``MdcContinuousLoadApplication``) and returns the service.
        ``result_prefix`` must be unique per burst because runner services are reused.
        :attr:`cache_defaults` are merged in.
        """
        load_params = {"mode": mode, "cacheName": cache_name, "resultPrefix": result_prefix,
                       **self.cache_defaults, **params}

        return self.run_app(dc, LOAD_APP, load_params, runner=runner)

    def control(self, dc: Optional[str] = None) -> ControlUtility:
        """
        :return: Control utility bound to the given DC's servers, the first DC by default.
        """
        return ControlUtility(self.servers[dc if dc is not None else self.dcs[0]])

    def verify_cache_distribution(self, cache_name: str, copies_per_dc: Optional[int] = None,
                                  dc: Optional[str] = None):
        """
        Verifies that every partition of the cache has an OWNING copy in every DC, and
        optionally that each DC holds exactly ``copies_per_dc`` copies.

        :return: The CacheDistribution for further custom assertions.
        """
        distribution = self.control(dc).cache_distribution(cache_names=cache_name, user_attributes=DATA_CENTER_ATTR)

        assert_cross_dc_distribution_by_attribute(distribution, dc_attr=DATA_CENTER_ATTR,
                                                  expected_dcs=self.dcs, copies_per_dc=copies_per_dc)

        return distribution

    def verify_split_brain(self):
        """
        Verifies that the network partition split the cluster into as many independent
        segments as there are DCs, i.e. every DC ended up on its own.
        """
        self.verify_segments(*self.dcs)

    def verify_segments(self, *segments: Segment):
        """
        Verifies that the cluster has split into exactly the given independent segments:
        every segment is healthy on its own, no two segments share a baseline node, and
        each segment elected its own coordinator.

        A segment is a DC name or a collection of DC names that still see each other, e.g.
        ``verify_segments((DC_1, DC_2), DC_3)`` for a cluster with DC3 cut off.
        """
        normalized = [_as_segment(segment) for segment in segments]

        for segment in normalized:
            self.verify_segment_healthy(segment)

        states = {segment: self.control(segment[0]).cluster_state() for segment in normalized}

        baselines = {segment: {node.consistent_id for node in states[segment].baseline} for segment in normalized}

        for seg_a, seg_b in combinations(normalized, 2):
            common_nodes = baselines[seg_a] & baselines[seg_b]

            assert not common_nodes, \
                f"Segment baselines should not intersect [common={sorted(common_nodes)}, " \
                f"{seg_a}={sorted(baselines[seg_a])}, {seg_b}={sorted(baselines[seg_b])}]"

        coordinators = {}

        for segment in normalized:
            coordinator = states[segment].coordinator

            assert coordinator, f"Coordinator is not found in the {segment} segment baseline output!"

            assert coordinator.consistent_id in baselines[segment], \
                f"{segment} coordinator should belong to its own segment baseline " \
                f"[coordinator={coordinator.consistent_id}, baseline={sorted(baselines[segment])}]"

            coordinators[segment] = coordinator.consistent_id

        assert len(set(coordinators.values())) == len(normalized), \
            f"Every segment should have elected its own coordinator [coordinators={coordinators}]"

    def verify_segment_healthy(self, segment: Segment):
        """
        Verifies that a segment is fully alive, ACTIVE, and its baseline covers exactly
        the servers of the DCs it consists of - and nothing else.
        """
        dcs = _as_segment(segment)

        exp_alive_nodes = sum(self.srv_per_dc[dc] for dc in dcs)
        act_alive_nodes = sum(len(self.servers[dc].alive_nodes) for dc in dcs)

        assert act_alive_nodes == exp_alive_nodes, \
            f"{exp_alive_nodes} nodes should be alive in {dcs}! [actual={act_alive_nodes}]"

        cluster_state = self.control(dcs[0]).cluster_state()

        assert "ACTIVE" == cluster_state.state, \
            f"{dcs} segment state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"{dcs} segment baseline is not expected " \
            f"[exp={exp_alive_nodes}, actual_baseline={cluster_state.baseline}]"

    def verify_whole_cluster_healthy(self):
        """
        Verifies that all DCs form a single ACTIVE cluster: every server node is alive
        and the baseline seen from the first DC covers all servers of every DC.
        """
        self.verify_segment_healthy(self.dcs)

    def verify_servers_log_clean(self):
        """
        Verifies the negative invariants on all server nodes: no long running transactions
        were detected, no PME hang and no lost partitions were reported.
        """
        for pattern in (LRT_PATTERN, PME_FREEZE_PATTERN, LOST_PARTITIONS_PATTERN, ASSERTION_ERROR_PATTERN):
            for svc in self.servers.values():
                svc.check_event_absent(pattern, log_file=ALL_LOGS_GLOB)

    def verify_no_hanging_txs(self, dc: Optional[str] = None, try_kill_hanging_tx: bool = False):
        """
        Verifies that no active transactions are left on the cluster.
        """
        txs = self.control(dc).tx()

        if isinstance(txs, list) and len(txs) > 0 and try_kill_hanging_tx:
            for tx in txs:
                self.control(dc).tx_kill(xid=tx.xid)

            txs = self.control(dc).tx()

        assert not isinstance(txs, list) or len(txs) == 0, f"No active transactions expected [txs={txs}]"

    @staticmethod
    def result_int(svc: IgniteApplicationService, name: str) -> int:
        """
        :return: Application-recorded integer result.
        """
        return int(svc.extract_result(name))

    @staticmethod
    def result_float(svc: IgniteApplicationService, name: str) -> float:
        """
        :return: Application-recorded float result.
        """
        return float(svc.extract_result(name))

    @staticmethod
    def result_bool(svc: IgniteApplicationService, name: str) -> bool:
        """
        :return: Application-recorded boolean result.
        """
        val = svc.extract_result(name).strip().lower()

        return val == "true"


def cross_dc_network(logger, mdc: MdcCluster, delay_ms: Optional[int] = None, loss: Optional[float] = None,
                     pairs: Optional[Dict[Tuple[str, str], CrossNetworkGroupConfiguration]] = None
                     ) -> NetworkGroupManager:
    """
    Builds a :class:`NetworkGroupManager` (context manager) for the cluster, applying the
    same impairment to every DC pair. With no impairments the manager still owns partition
    enable/disable and the final network cleanup.

    :param delay_ms: One-way cross-DC latency in milliseconds (the effective RTT is twice
           that, since netem delay is applied on egress in both directions).
    :param loss: Cross-DC packet loss fraction in [0.0, 1.0].
    :param pairs: Per-pair overrides of the symmetric profile, for a cluster whose WAN
           links are not all alike, e.g. ``{(DC_1, DC_3): CrossNetworkGroupConfiguration(delay="200ms")}``.
           An empty configuration leaves that pair unimpaired.
    """
    dflt = CrossNetworkGroupConfiguration(delay=f"{delay_ms}ms" if delay_ms is not None else None, loss=loss)

    matrix = {frozenset(pair): dflt for pair in all_pairs(mdc.dcs)}

    for pair, cfg in (pairs or {}).items():
        matrix[frozenset(pair)] = cfg

    store = NetworkGroupStore()

    for pair, cfg in matrix.items():
        if not cfg.is_empty:
            store.set_config(*pair, cfg)

    return NetworkGroupManager(logger, store, mdc.network_registry())


def assert_cross_dc_distribution_by_attribute(distribution, dc_attr, expected_dcs, owning_only=True,
                                              copies_per_dc=None):
    """
    Asserts that every partition of every cache group has at least one copy in every DC,
    using a node attribute (requested via --user-attributes) as the DC marker.

    :param distribution: CacheDistribution returned by ControlUtility.cache_distribution(),
                         requested with user_attributes=[dc_attr].
    :param dc_attr: Attribute name holding the DC id, e.g. "IGNITE_DATA_CENTER_ID".
    :param expected_dcs: Collection of DC ids that must own a copy of every partition.
    :param owning_only: Count only copies in OWNING state as present.
    :param copies_per_dc: If set, each DC must hold exactly this many copies of every
                          partition - the MdcAffinityBackupFilter guarantee
                          ``(backups + 1) / dcsNum``.
    """
    def dc_of(copy):
        return copy.user_attributes.get(dc_attr)

    _assert_cross_dc(distribution, set(expected_dcs), dc_of, owning_only, copies_per_dc,
                     layout_hint=f"DC attribute: {dc_attr}, expected DCs: {sorted(expected_dcs)}")


def _assert_cross_dc(distribution, expected_dcs, dc_of, owning_only, copies_per_dc, layout_hint):
    violations = []

    for group in distribution.groups.values():
        for part, copies in sorted(group.partitions.items()):
            counted = [c for c in copies if not owning_only or c.state == "OWNING"]

            per_dc = {dc: 0 for dc in expected_dcs}

            for copy in counted:
                dc = dc_of(copy)

                if dc in per_dc:
                    per_dc[dc] += 1

            missing = {dc for dc, cnt in per_dc.items() if cnt == 0}

            unbalanced = {} if copies_per_dc is None else \
                {dc: cnt for dc, cnt in per_dc.items() if cnt != copies_per_dc}

            if missing or unbalanced:
                copies_dump = ", ".join(
                    f"{c.node_id}({'P' if c.primary else 'B'},{c.state},dc={dc_of(c)},{c.node_addresses})"
                    for c in copies)

                problems = []

                if missing:
                    problems.append(f"missing DCs={sorted(missing)}")

                if unbalanced:
                    problems.append(f"copies per DC != {copies_per_dc}: {unbalanced}")

                violations.append(f"group={group.name}(id={group.group_id}), partition={part}, "
                                  f"{', '.join(problems)}, copies=[{copies_dump}]")

    assert not violations, \
        "Partition distribution is not cross-DC:\n  " + "\n  ".join(violations) + "\n" + layout_hint
