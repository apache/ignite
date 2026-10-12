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

    mdc = MdcCluster(self, ignite_version, dcs=DCS_3, srv_per_dc=2, runners_per_dc=1)

    with cross_dc_network(self.logger, mdc, delay_ms=20) as net:
        mdc.start_servers()
        mdc.generate_data(DC_1, CACHE, 0, 1000)
        mdc.verify_cache_distribution(CACHE, copies_per_dc=1)

        net.enable_network_partition(DC_1, DC_2)
        ...

Globals:

    mdc_cache_topology_validator - whether the MDC caches are created with the cache level
    MdcTopologyValidator, default true.
"""
from itertools import combinations
from typing import Dict, List, Optional, Sequence, Union

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.mdc.mdc_utils import DATA_CENTER_ATTR, dc_jvm_opts, per_dc, \
    assert_cross_dc_distribution_by_attribute
from ignitetest.services.network_group.configuration import NetworkGroupStore, CrossNetworkGroupConfiguration
from ignitetest.services.network_group.manager import NetworkGroupManager
from ignitetest.services.utils.control_utility import ControlUtility
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.ignite_configuration.discovery import TcpDiscoverySpi, TcpDiscoveryVmIpFinder
from ignitetest.services.utils.ssl.client_connector_configuration import ClientConnectorConfiguration
from ignitetest.utils.version import IgniteVersion

DC_1 = "DC1"
DC_2 = "DC2"
DC_3 = "DC3"

DCS_2 = (DC_1, DC_2)
DCS_3 = (DC_1, DC_2, DC_3)

IGNITE_STARTUP_TIMEOUT_SEC = 90

CACHE_TOP_VALIDATOR_GLOBAL = "mdc_cache_topology_validator"

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

# Put admissibility check N writes keys from N * PROBE_KEY_STEP on, far above any data set.
PROBE_KEY_STEP = 1_000_000


class MdcCluster:
    """
    Owns the per-DC Ignite services and reusable application services of an MDC test,
    plus the MDC-specific verification helpers.

    :param test: The ducktape test instance.
    :param ignite_version: Ignite version string.
    :param dcs: Data centers the cluster spans; the first one is the main DC of an even DC set.
    :param srv_per_dc: Servers per DC, an int or a per-DC dict (asymmetric DCs).
    :param runners_per_dc: Reusable run-to-completion app services per DC (generator,
           checkers, load bursts). An int or a per-DC dict.
    :param loaders_per_dc: Dedicated background load app services per DC. They run
           concurrently with runner apps, hence separate containers.
    :param client_connector: Whether to expose the thin client connector on servers.
    :param network_timeout: Network timeout of the nodes, in ms.
    :param discovery_network_timeout: Network timeout of the discovery SPI, in ms.
    """
    def __init__(self, test, ignite_version: str, dcs: Sequence[str] = DCS_2,
                 srv_per_dc: Union[int, Dict[str, int]] = 3,
                 runners_per_dc: Union[int, Dict[str, int]] = 1,
                 loaders_per_dc: Union[int, Dict[str, int]] = 0,
                 client_connector: bool = False,
                 network_timeout: Optional[int] = None,
                 discovery_network_timeout: Optional[int] = None):
        self.test_context = test.test_context
        self.logger = test.logger

        self.dcs = tuple(dcs)

        self.discovery_network_timeout = discovery_network_timeout

        self.ignite_config = self._ignite_config(ignite_version, client_connector, network_timeout)

        self.srv_per_dc = per_dc(srv_per_dc, self.dcs)

        self.servers: Dict[str, IgniteService] = {
            dc: IgniteService(self.test_context, self.ignite_config, num_nodes=num, jvm_opts=dc_jvm_opts(dc),
                              startup_timeout_sec=IGNITE_STARTUP_TIMEOUT_SEC)
            for dc, num in self.srv_per_dc.items() if num > 0}

        # Every DC discovers through all of them, so any DC can restart and rejoin the others.
        server_spi = self._discovery_spi([node for svc in self.servers.values() for node in svc.nodes])

        for svc in self.servers.values():
            svc.config = svc.config._replace(discovery_spi=server_spi)

        self.runners: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in per_dc(runners_per_dc, self.dcs).items()}

        self.loaders: Dict[str, List[IgniteApplicationService]] = {
            dc: [self._app_service(dc) for _ in range(num)]
            for dc, num in per_dc(loaders_per_dc, self.dcs).items()}

        # Extra services (e.g. thin clients) registered into a DC's network group.
        self.extras: Dict[str, List] = {dc: [] for dc in self.dcs}

        # App services that have been started at least once: the first start is clean,
        # subsequent ones preserve work dirs (and logs - hence unique result prefixes).
        self._started_apps = set()

        # Admissibility checks run on reusable services, so each check needs a unique result prefix.
        self._adm_checks = 0

    @staticmethod
    def _ignite_config(ignite_version: str, client_connector: bool,
                       network_timeout: Optional[int]) -> IgniteConfiguration:
        """
        :return: Configuration shared by the servers and clients of every DC.
        """
        cfg_kwargs = {"version": IgniteVersion(ignite_version)}

        if network_timeout is not None:
            cfg_kwargs["network_timeout"] = network_timeout

        if client_connector:
            cfg_kwargs["client_connector_configuration"] = ClientConnectorConfiguration()

        return IgniteConfiguration(**cfg_kwargs)

    def _discovery_spi(self, nodes) -> TcpDiscoverySpi:
        """
        :return: Discovery SPI that joins through the given nodes, one port per host.
        """
        return TcpDiscoverySpi(ip_finder=TcpDiscoveryVmIpFinder(nodes), port_range=0,
                               network_timeout=self.discovery_network_timeout)

    def _app_service(self, dc: str) -> IgniteApplicationService:
        client_cfg = self.ignite_config._replace(client_mode=True,
                                                 discovery_spi=self._discovery_spi(self.servers[dc].nodes))

        return IgniteApplicationService(self.test_context, client_cfg, jvm_opts=dc_jvm_opts(dc))

    @property
    def min_backups(self) -> int:
        """
        :return: Fewest backups that give every DC a copy of every partition.
        """
        return len(self.dcs) - 1

    def _cache_params(self) -> dict:
        """
        :return: Parameters of an MDC cache that match the cluster's DCs.
        """
        params = {"dcsNum": len(self.dcs),
                  "topologyValidator": self.test_context.globals.get(CACHE_TOP_VALIDATOR_GLOBAL, True)}

        # The validator checks an odd DC set by majority and an even one by the main DC.
        if len(self.dcs) % 2 == 1:
            params["datacenters"] = list(self.dcs)
        else:
            params["mainDc"] = self.dcs[0]

        return params

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

    def describe(self) -> List[str]:
        """
        Describes the cluster per data center for a demo breakpoint banner
        (see :meth:`ignitetest.utils.ignite_test.IgniteTest.pause`). The generic banner sees
        a flat list of services, which is where the DC each node belongs to gets lost.

        Structure only - which node is up is what the banner's own service section reports,
        and it pays an SSH probe per node to find out.

        :return: Section lines, the first one being the section title.
        """
        lines = ["DATA CENTERS"]

        for dc in self.dcs:
            roles = [(label, [node.account.hostname for svc in services for node in svc.nodes])
                     for label, services in (("server", [self.servers[dc]] if dc in self.servers else []),
                                             ("runner", self.runners.get(dc, [])),
                                             ("loader", self.loaders.get(dc, [])),
                                             ("extra", self.extras.get(dc, [])))]

            # A DC that holds nothing is not named at all: an empty header reads as a DC whose
            # nodes have gone, which is exactly what a partition demo is being watched for.
            if not any(hosts for _, hosts in roles):
                continue

            lines.append(f"  {dc}")
            lines.extend(f"    {label:<7} {' '.join(hosts)}" for label, hosts in roles if hosts)

        return lines

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

        svc.params = {**self._cache_params(), **params}

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
        svc.params = {**self._cache_params(), **params}

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

    def generate_data(self, dc: str, cache_name: str, from_idx: int, to_idx: int, backups: Optional[int] = None,
                      sql_mode: bool = False, **cache_params) -> IgniteApplicationService:
        """
        Creates the MDC cache (if absent) and populates keys ``[from_idx, to_idx)``.
        Extra cache parameters (``atomicity``, ``writeSync``, ``readFromBackup``,
        ``partitions``, ...) are passed through to the cache configuration builder.

        :param backups: Backups, :attr:`min_backups` by default.
        """
        params = {"cacheName": cache_name, "backups": self.min_backups if backups is None else backups,
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
                                key_offset: Optional[int] = None, probes: int = 100) -> IgniteApplicationService:
        """
        Verifies that puts from the given DC are accepted by the topology validator, or
        rejected by it. An admissible check fails on the first rejected put, an inadmissible
        one if any put succeeds. Each check writes its own key range unless ``key_offset``
        is given.
        """
        self._adm_checks += 1

        if key_offset is None:
            key_offset = self._adm_checks * PROBE_KEY_STEP

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
                                                  expected_dcs=self.dcs, copies_per_dc=copies_per_dc)

        return distribution

    def verify_split_brain(self, *segments: Union[str, Sequence[str]]):
        """
        Verifies that the cluster split into exactly the given segments, one per DC by default.
        A segment is a DC or a tuple of DCs that still see each other.
        """
        segments = [(seg,) if isinstance(seg, str) else tuple(seg) for seg in segments or self.dcs]

        states = {seg: self.verify_segment_healthy(*seg) for seg in segments}

        baselines = {seg: {node.consistent_id for node in states[seg].baseline} for seg in segments}

        for seg_a, seg_b in combinations(segments, 2):
            common_nodes = baselines[seg_a] & baselines[seg_b]

            assert not common_nodes, \
                f"Segment baselines should not intersect [common={sorted(common_nodes)}, " \
                f"{seg_a}={sorted(baselines[seg_a])}, {seg_b}={sorted(baselines[seg_b])}]"

        for seg in segments:
            coordinator = states[seg].coordinator

            assert coordinator, f"Coordinator is not found in the {seg} segment baseline output!"

            assert coordinator.consistent_id in baselines[seg], \
                f"{seg} coordinator should belong to its own segment baseline " \
                f"[coordinator={coordinator.consistent_id}, baseline={sorted(baselines[seg])}]"

    def verify_segment_healthy(self, *dcs: str):
        """
        Verifies that the servers of the given DCs are alive and form an ACTIVE cluster whose
        baseline holds exactly them.

        :return: The cluster state read from the segment.
        """
        exp_alive_nodes = sum(self.srv_per_dc.get(dc, 0) for dc in dcs)
        act_alive_nodes = sum(len(self.servers[dc].alive_nodes) for dc in dcs if dc in self.servers)

        assert act_alive_nodes == exp_alive_nodes, \
            f"{exp_alive_nodes} nodes should be alive in {dcs}! [actual={act_alive_nodes}]"

        cluster_state = self.control(dcs[0]).cluster_state()

        assert "ACTIVE" == cluster_state.state, \
            f"{dcs} segment state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"{dcs} segment baseline is not expected " \
            f"[exp={exp_alive_nodes}, actual_baseline={cluster_state.baseline}]"

        return cluster_state

    def verify_whole_cluster_healthy(self):
        """
        Verifies that all DCs form a single ACTIVE cluster: every server node is alive
        and the baseline covers all servers of every DC.
        """
        self.verify_segment_healthy(*self.dcs)

    def verify_servers_log_clean(self):
        """
        Verifies the negative invariants on all server nodes: no long running transactions
        were detected, no PME hang and no lost partitions were reported.
        """
        for pattern in (LRT_PATTERN, PME_FREEZE_PATTERN, LOST_PARTITIONS_PATTERN, ASSERTION_ERROR_PATTERN):
            for svc in self.servers.values():
                svc.check_event_absent(pattern, log_file=ALL_LOGS_GLOB)

    def verify_no_hanging_txs(self, dc: str = DC_1, try_kill_hanging_tx: bool = False):
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


def cross_dc_network(logger, mdc: MdcCluster, delay_ms: Optional[int] = None,
                     loss: Optional[float] = None) -> NetworkGroupManager:
    """
    Builds a :class:`NetworkGroupManager` (context manager) for the cluster with the same
    impairments on every DC pair. With no impairments the manager still owns partition
    enable/disable and the final network cleanup.

    :param delay_ms: One-way cross-DC latency in milliseconds (the effective RTT is twice
           that, since netem delay is applied on egress in both directions).
    :param loss: Cross-DC packet loss fraction in [0.0, 1.0].
    """
    store = NetworkGroupStore()

    cfg = CrossNetworkGroupConfiguration(delay=f"{delay_ms}ms" if delay_ms is not None else None, loss=loss)

    if not cfg.is_empty:
        for dc_a, dc_b in combinations(mdc.dcs, 2):
            store.set_config(dc_a, dc_b, cfg)

    return NetworkGroupManager(logger, store, mdc.network_registry())
