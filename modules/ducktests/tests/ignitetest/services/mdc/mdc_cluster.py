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
        mdc.generate_data(DC_1, CACHE, 0, 1000, backups=1)
        mdc.verify_cache_distribution(CACHE, copies_per_dc=1)

        net.enable_network_partition(DC_1, DC_2)
        ...
"""
from typing import Dict, List, Optional, Union

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
DCS = (DC_1, DC_2)

IGNITE_STARTUP_TIMEOUT_SEC = 90

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


def dc_jvm_opts(dc: str) -> List[str]:
    """
    :return: JVM options assigning a node to the given data center.
    """
    return [f"-D{DATA_CENTER_ATTR}={dc}", f"-D{IGNITE_SQL_RETRY_TIMEOUT_ATTR}={IGNITE_SQL_RETRY_TIMEOUT_MS}"]


def _per_dc(value: Union[int, Dict[str, int]]) -> Dict[str, int]:
    """
    Normalizes an int-or-dict per-DC count into a dict, e.g. 3 -> {DC1: 3, DC2: 3}.
    """
    return dict(value) if isinstance(value, dict) else {dc: value for dc in DCS}


class MdcCluster:
    """
    Owns the per-DC Ignite services and reusable application services of an MDC test,
    plus the MDC-specific verification helpers.

    :param test: The ducktape test instance.
    :param ignite_version: Ignite version string.
    :param srv_per_dc: Servers per DC, an int or a per-DC dict (asymmetric DCs).
    :param runners_per_dc: Reusable run-to-completion app services per DC (generator,
           checkers, load bursts). An int or a per-DC dict.
    :param loaders_per_dc: Dedicated background load app services per DC. They run
           concurrently with runner apps, hence separate containers.
    :param client_connector: Whether to expose the thin client connector on servers.
    """
    def __init__(self, test, ignite_version: str, srv_per_dc: Union[int, Dict[str, int]] = 3,
                 runners_per_dc: Union[int, Dict[str, int]] = 1,
                 loaders_per_dc: Union[int, Dict[str, int]] = 0,
                 client_connector: bool = False,
                 network_timeout: int = 5_000,
                 tcp_connect_timeout: int = 5_000):
        self.test_context = test.test_context
        self.logger = test.logger

        # A single discovery SPI (hence a single ip finder) shared by both DCs' server
        # services is what makes the two DCs form ONE cluster: prepare_on_start()
        # memoizes the addresses of the first started DC into the shared ip finder, so
        # the second DC discovers through the first DC's nodes, and restart() re-joins
        # the same way. Restarting the first started DC itself is the one case this
        # breaks - see sync_service_discovery().
        cfg_kwargs = {
            "version": IgniteVersion(ignite_version),
            "discovery_spi": TcpDiscoverySpi(),
            "network_timeout": network_timeout,
            "communication_spi": TcpCommunicationSpi(connect_timeout=tcp_connect_timeout)
        }

        if client_connector:
            cfg_kwargs["client_connector_configuration"] = ClientConnectorConfiguration()

        self.ignite_config = IgniteConfiguration(**cfg_kwargs)

        self.srv_per_dc = _per_dc(srv_per_dc)

        self.servers: Dict[str, IgniteService] = {
            dc: IgniteService(self.test_context, self.ignite_config, num_nodes=num, jvm_opts=dc_jvm_opts(dc),
                              startup_timeout_sec=IGNITE_STARTUP_TIMEOUT_SEC)
            for dc, num in self.srv_per_dc.items() if num > 0}

        self.runners: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in _per_dc(runners_per_dc).items()}

        self.loaders: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in _per_dc(loaders_per_dc).items()}

        # Extra services (e.g. thin clients) registered into a DC's network group.
        self.extras: Dict[str, List] = {dc: [] for dc in DCS}

        # App services that have been started at least once: the first start is clean,
        # subsequent ones preserve work dirs (and logs - hence unique result prefixes).
        self._started_apps = set()

        # Admissibility checks run on reusable services, so each check needs a unique result prefix.
        self._adm_checks = 0

    def sync_service_discovery(self):
        """
        Points every server service at a discovery SPI covering all DCs.

        Required before restarting the FIRST started DC: the shared ip finder holds only
        that DC's addresses, so after a full stop its nodes would seed off themselves and
        form a separate cluster instead of rejoining the surviving DC.
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

        for dc in DCS:
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
        read-only half-ring back into the main cluster after a partition heals).
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

        svc.params = params

        svc.start(clean=self._first_start(svc))
        svc.wait()
        svc.stop()

        return svc

    def start_loader(self, dc: str, params: dict, loader: int = 0,
                     java_class: str = LOAD_APP) -> IgniteApplicationService:
        """
        Starts a background load application (runs until stopped). Any exception raised
        by the application surfaces in :meth:`stop_loader`.
        """
        svc = self.loaders[dc][loader]

        svc.java_class_name = java_class
        svc.params = params

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

    def _first_start(self, svc) -> bool:
        first = id(svc) not in self._started_apps

        self._started_apps.add(id(svc))

        return first

    def generate_data(self, dc: str, cache_name: str, from_idx: int, to_idx: int, backups: int,
                      main_dc: str = DC_1, sql_mode: bool = False, **cache_params) -> IgniteApplicationService:
        """
        Creates the MDC cache (if absent) and populates keys ``[from_idx, to_idx)``.
        Extra cache parameters (``atomicity``, ``writeSync``, ``readFromBackup``,
        ``partitions``, ...) are passed through to the cache configuration builder.
        """
        params = {"cacheName": cache_name, "backups": backups, "mainDc": main_dc,
                  "from": from_idx, "to": to_idx, "sqlMode": sql_mode, **cache_params}

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
        Verifies that put load from the given DC is admissible (primary DC visible) or
        rejected by the topology validator (read-only DC). A PUT burst of the load
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
        """
        load_params = {"mode": mode, "cacheName": cache_name, "resultPrefix": result_prefix, **params}

        return self.run_app(dc, LOAD_APP, load_params, runner=runner)

    def control(self, dc: str = DC_1) -> ControlUtility:
        """
        :return: Control utility bound to the given DC's servers.
        """
        return ControlUtility(self.servers[dc])

    def verify_cache_distribution(self, cache_name: str, copies_per_dc: Optional[int] = None, dc: str = DC_1):
        """
        Verifies that every partition of the cache has an OWNING copy in every DC, and
        optionally that each DC holds exactly ``copies_per_dc`` copies.

        :return: The CacheDistribution for further custom assertions.
        """
        distribution = self.control(dc).cache_distribution(cache_names=cache_name, user_attributes=DATA_CENTER_ATTR)

        assert_cross_dc_distribution_by_attribute(distribution, dc_attr=DATA_CENTER_ATTR,
                                                  expected_dcs=DCS, copies_per_dc=copies_per_dc)

        return distribution

    def verify_split_brain(self):
        """
        Verifies that after the network partition the cluster has split into two independent
        half-rings: their baselines don't intersect and each half elected its own coordinator.
        """
        for dc in DCS:
            self.verify_half_ring_healthy(dc)

        state = {dc: self.control(dc).cluster_state() for dc in DCS}

        baselines = {dc: {node.consistent_id for node in state[dc].baseline} for dc in DCS}

        common_nodes = baselines[DC_1] & baselines[DC_2]

        assert not common_nodes, \
            f"Half-ring baselines should not intersect " \
            f"[common={sorted(common_nodes)}, dc1={sorted(baselines[DC_1])}, dc2={sorted(baselines[DC_2])}]"

        for dc in DCS:
            coordinator = state[dc].coordinator

            assert coordinator, f"Coordinator is not found in {dc} half-ring baseline output!"

            assert coordinator.consistent_id in baselines[dc], \
                f"{dc} coordinator should belong to its own half-ring baseline " \
                f"[coordinator={coordinator.consistent_id}, baseline={sorted(baselines[dc])}]"

        assert state[DC_1].coordinator.consistent_id != state[DC_2].coordinator.consistent_id, \
            f"Half-rings should have different coordinators " \
            f"[coordinator={state[DC_1].coordinator.consistent_id}]"

    def verify_half_ring_healthy(self, dc: str):
        """
        Verifies that a half-ring is fully alive, ACTIVE, and its baseline matches its size.
        """
        exp_alive_nodes = self.srv_per_dc[dc]
        act_alive_nodes = len(self.servers[dc].alive_nodes)

        assert act_alive_nodes == exp_alive_nodes, \
            f"{exp_alive_nodes} nodes should be alive in {dc}! [actual={act_alive_nodes}]"

        cluster_state = self.control(dc).cluster_state()

        assert "ACTIVE" == cluster_state.state, \
            f"{dc} half-ring state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"{dc} half-ring baseline is not expected " \
            f"[exp={exp_alive_nodes}, actual_baseline={cluster_state.baseline}]"

    def verify_whole_cluster_healthy(self):
        """
        Verifies that both DCs form a single ACTIVE cluster: every server node is alive
        and the baseline seen from DC1 covers all servers of both DCs.
        """
        exp_total = sum(self.srv_per_dc.values())

        act_alive = sum(len(self.servers[dc].alive_nodes) for dc in self.servers)

        assert act_alive == exp_total, f"All {exp_total} server nodes should be alive [actual={act_alive}]"

        cluster_state = self.control(DC_1).cluster_state()

        assert "ACTIVE" == cluster_state.state, f"Cluster should be ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_total, \
            f"Cluster baseline should cover both DCs [exp={exp_total}, actual={cluster_state.baseline}]"

    def verify_servers_log_clean(self):
        """
        Verifies the negative invariants on all server nodes: no long running transactions
        were detected, no PME hang and no lost partitions were reported.
        """
        for pattern in (LRT_PATTERN, PME_FREEZE_PATTERN, LOST_PARTITIONS_PATTERN, ASSERTION_ERROR_PATTERN):
            for svc in self.servers.values():
                svc.check_event_absent(pattern, log_file=ALL_LOGS_GLOB)

    def verify_no_hanging_txs(self, dc: str = DC_1):
        """
        Verifies that no active transactions are left on the cluster.
        """
        txs = self.control(dc).tx()

        # ControlUtility.tx() returns a list of parsed transactions, or the raw command
        # output (a str) when nothing parsed - i.e. when there are no transactions.
        assert not isinstance(txs, list) or not txs, f"No active transactions expected [txs={txs}]"

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


def cross_dc_network(logger, mdc: MdcCluster, delay_ms: Optional[int] = None,
                     loss: Optional[float] = None) -> NetworkGroupManager:
    """
    Builds a :class:`NetworkGroupManager` (context manager) for the cluster with symmetric
    DC1 <-> DC2 impairments. With no impairments the manager still owns partition
    enable/disable and the final network cleanup.

    :param delay_ms: One-way cross-DC latency in milliseconds (the effective RTT is twice
           that, since netem delay is applied on egress in both directions).
    :param loss: Cross-DC packet loss fraction in [0.0, 1.0].
    """
    store = NetworkGroupStore()

    cfg = CrossNetworkGroupConfiguration(delay=f"{delay_ms}ms" if delay_ms is not None else None, loss=loss)

    if not cfg.is_empty:
        store.set_config(DC_1, DC_2, cfg)

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
