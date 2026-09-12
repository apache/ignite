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
from ignitetest.services.utils.jmx_utils import JmxClient, metric_registry_pattern
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

# Needed by everything that reads a node metric over JMX - await_rebalance(), the snapshot
# commands and the MDC safety metrics below among them.
JMX_METRIC_EXPORTER = "org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi"

# Per-cache metrics holding the cluster's own verdict on the MDC guarantees. Registered on
# every server node that carries a DC id, for every cache.
#
# The two are not the same statement. The affinity one is about CONFIGURATION - whether the
# cache is set up to keep a copy of every partition in every DC at all, which is what the
# MdcAffinityBackupFilter provides. The distribution one is about the CURRENT assignment
# actually doing so, which a correctly configured cache still fails while some DC has no
# nodes to place a copy on.
MDC_SAFE_AFFINITY_METRIC = "IsCacheAffinityConfigurationMdcSafe"
MDC_SAFE_DISTRIBUTION_METRIC = "IsCachePartitionDistributionSafe"

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


def per_dc(value: Union[int, Dict[str, int]], dcs: Sequence[str]) -> Dict[str, int]:
    """
    Normalizes an int-or-dict per-DC count into a dict, e.g. 3 -> {DC1: 3, DC2: 3}.

    A dict naming a DC the cluster does not span is rejected here rather than left to fail
    later: such a DC is skipped by :meth:`MdcCluster.network_registry`, so its nodes would
    run with no impairments and no partition rules while every other call site kept working.
    """
    if not isinstance(value, dict):
        return {dc: value for dc in dcs}

    unknown = sorted(dc for dc in value if dc not in dcs)

    assert not unknown, \
        f"Per-DC counts name data centers the cluster does not span [unknown={unknown}, dcs={list(dcs)}]"

    return dict(value)


def _as_segment(segment: Segment) -> Tuple[str, ...]:
    """
    Normalizes a single DC name or a collection of DC names into a tuple of DC names.
    """
    return (segment,) if isinstance(segment, str) else tuple(segment)


def _fmt_segment(segment: Tuple[str, ...]) -> str:
    """
    :return: Segment rendered for an assertion message, e.g. "DC1+DC2" - a Python tuple
             reads poorly in the middle of one, and a single DC renders as itself.
    """
    return "+".join(segment)


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
    :param jmx_metrics: Whether to export the node metrics over JMX. Required by everything
           that reads one - see :meth:`cache_mdc_metrics`.
    """
    def __init__(self, test, ignite_version: str, dcs: Sequence[str] = DCS_2,
                 main_dc: Optional[str] = None,
                 srv_per_dc: Union[int, Dict[str, int]] = 3,
                 runners_per_dc: Union[int, Dict[str, int]] = 1,
                 loaders_per_dc: Union[int, Dict[str, int]] = 0,
                 client_connector: bool = False,
                 jmx_metrics: bool = False,
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

        if jmx_metrics:
            # A fresh set, never the shared mutable default of IgniteConfiguration.
            cfg_kwargs["metric_exporters"] = {JMX_METRIC_EXPORTER}

        self.ignite_config = IgniteConfiguration(**cfg_kwargs)

        self.srv_per_dc = per_dc(srv_per_dc, self.dcs)

        self.servers: Dict[str, IgniteService] = {
            dc: self._server_service(dc, num) for dc, num in self.srv_per_dc.items() if num > 0}

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

        # Cache parameters applied to every cache this fixture creates, unless a call overrides them.
        self.cache_defaults = {
            "topologyValidator": self.test_context.globals.get(CACHE_TOP_VALIDATOR_GLOBAL, True)
        }

        self.logger.info(f"MDC cache defaults [{self.cache_defaults}]")

    def _server_service(self, dc: str, num_nodes: int) -> IgniteService:
        """
        Builds the server service of one DC. The single place a server command line is put
        together, so a fork that starts its servers differently overrides just this.
        """
        return IgniteService(self.test_context, self.ignite_config, num_nodes=num_nodes,
                             jvm_opts=dc_jvm_opts(dc), startup_timeout_sec=IGNITE_STARTUP_TIMEOUT_SEC)

    def dc_servers(self, dc: str) -> List[IgniteService]:
        """
        :return: All server services of the given DC - one, unless a subclass splits a DC's
                 servers into several services (node groups, cells, availability zones).
        """
        return [self.servers[dc]] if dc in self.servers else []

    def all_servers(self) -> List[IgniteService]:
        """
        :return: Every server service of the cluster, DCs in order.
        """
        return [svc for dc in sorted(self.servers) for svc in self.dc_servers(dc)]

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
        discovery_spi = from_ignite_services(self.all_servers())

        for service in self.all_servers():
            service.config = service.config._replace(discovery_spi=discovery_spi)

    def _app_service(self, dc: str) -> IgniteApplicationService:
        # Seeding off the DC's first server service is enough: all of them are one cluster.
        client_cfg = self.ignite_config._replace(client_mode=True,
                                                 discovery_spi=from_ignite_cluster(self.dc_servers(dc)[0]))

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
            services = list(self.dc_servers(dc))

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
                     for label, services in (("server", self.dc_servers(dc)),
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

        return [f"{node.account.hostname}:{port}" for svc in self.all_servers() for node in svc.nodes]

    def start_servers(self):
        """
        Starts all server services.
        """
        for svc in self.all_servers():
            svc.start()

    def stop_servers(self):
        """
        Stops all server services.
        """
        for svc in self.all_servers():
            svc.stop()

    def stop_dcs(self, *dcs: str):
        """
        Stops the server services of the given DCs, in the order given - a data center
        outage, as opposed to the network partition :func:`cross_dc_network` produces.
        """
        for dc in dcs:
            for svc in self.dc_servers(dc):
                svc.stop()

    def start_dcs(self, *dcs: str, clean: bool = False, await_rebalance: bool = True):
        """
        Starts the given DCs back, by default preserving their persistence, and waits until
        the cluster has rebalanced onto them.

        Every DC is started before the first wait, so that their joins are not serialized
        behind each other's rebalance.

        Restarting the FIRST started DC needs :meth:`sync_service_discovery` beforehand.
        """
        for dc in dcs:
            for svc in self.dc_servers(dc):
                svc.start(clean=clean)

        if await_rebalance:
            for dc in dcs:
                for svc in self.dc_servers(dc):
                    svc.await_rebalance()

    def restart(self, dc: str, clean: bool = False, await_rebalance: bool = True):
        """
        Restarts a whole DC preserving its persistence (the pattern used to rejoin a
        read-only segment back into the main cluster after a partition heals).
        """
        self.stop_dcs(dc)

        self.start_dcs(dc, clean=clean, await_rebalance=await_rebalance)

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

        svc.params = self._with_cache_params(params)

        svc.start(clean=self._first_start(svc))
        svc.wait()
        svc.stop()

        return svc

    def start_loader(self, dc: str, params: dict, loader: int = 0,
                     java_class: str = LOAD_APP) -> IgniteApplicationService:
        """
        Starts a background load application (runs until stopped). Any exception raised
        by the application surfaces in :meth:`stop_loader`. A load that creates the cache
        (``createCache``) has the MDC cache parameters injected - see
        :meth:`_with_cache_params`.
        """
        svc = self.loaders[dc][loader]

        svc.java_class_name = java_class
        svc.params = self._with_cache_params(params)

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

    def _with_cache_params(self, params: dict, creates_cache: bool = False) -> dict:
        """
        Injects everything the MDC cache is configured from - the topology validator mode,
        the DC count the affinity backup filter needs, and :attr:`cache_defaults` - into
        the parameters of an application that creates it, so no call site can configure a
        cache that disagrees with the DC set. Explicit parameters still win.

        The single injection point for all of it: an application that does not create the
        cache is handed none of it, since it would only ever be ignored.

        :param creates_cache: Whether the application always creates the cache. The ones
               that decide at run time say so with a ``createCache`` parameter instead.
        """
        if not (creates_cache or params.get("createCache")):
            return params

        return {**self.topology_params(), **self.cache_defaults, **params}

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
        of the MDC cache parameters - see :meth:`_with_cache_params`.

        :param backups: Backup count, by default the smallest one that gives every DC a
               single copy of every partition (see :attr:`min_backups`).
        """
        params = {"cacheName": cache_name,
                  "backups": self.min_backups if backups is None else backups,
                  "from": from_idx, "to": to_idx, "sqlMode": sql_mode,
                  **cache_params}

        # The generator always creates the cache, so it carries no createCache parameter
        # for _with_cache_params() to key off.
        return self.run_app(dc, GENERATOR_APP, self._with_cache_params(params, creates_cache=True))

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
        A burst that creates the cache (``createCache``) has the MDC cache parameters
        injected - see :meth:`_with_cache_params`.
        """
        load_params = {"mode": mode, "cacheName": cache_name, "resultPrefix": result_prefix, **params}

        return self.run_app(dc, LOAD_APP, load_params, runner=runner)

    def control(self, dc: Optional[str] = None) -> ControlUtility:
        """
        :return: Control utility bound to the given DC's servers, the first DC by default.
        """
        return ControlUtility(self.dc_servers(dc if dc is not None else self.dcs[0])[0])

    def cache_mdc_metrics(self, cache_name: str, dc: Optional[str] = None) -> Dict[str, bool]:
        """
        Reads the cache's MDC safety metrics off a server node over JMX - see
        :data:`MDC_SAFE_AFFINITY_METRIC` and :data:`MDC_SAFE_DISTRIBUTION_METRIC` for what
        each of them claims.

        Requires the cluster to have been built with ``jmx_metrics=True``, since the metrics
        are only exposed by the JMX metric exporter.

        :param cache_name: Cache to read the metrics of.
        :param dc: DC whose node answers, the first one by default. Every server node reports
               the same verdict, so this only matters for a partitioned cluster - where each
               segment answers about the topology IT can see.
        :return: Metric name -> value.
        """
        node = next(node for svc in self.dc_servers(dc if dc is not None else self.dcs[0])
                    for node in svc.alive_nodes)

        mbean = JmxClient(node).find_mbean(metric_registry_pattern('cache', cache_name))

        return {name: mbean.bool_value(name)
                for name in (MDC_SAFE_AFFINITY_METRIC, MDC_SAFE_DISTRIBUTION_METRIC)}

    def verify_cache_mdc_metrics(self, cache_name: str, affinity_safe: Optional[bool] = None,
                                 distribution_safe: Optional[bool] = None, dc: Optional[str] = None):
        """
        Verifies the MDC safety metrics of a cache against what the scenario expects. Both
        expectations are optional: a metric left as None is only reported, which is how a
        scenario reads out a value whose verdict depends on the state of the topology rather
        than on the point being made.

        :return: The metrics that were read.
        """
        metrics = self.cache_mdc_metrics(cache_name, dc)

        self.logger.info(f"MDC safety metrics [cache={cache_name}, dc={dc}, {metrics}]")

        for name, expected in ((MDC_SAFE_AFFINITY_METRIC, affinity_safe),
                               (MDC_SAFE_DISTRIBUTION_METRIC, distribution_safe)):
            if expected is not None:
                assert metrics[name] == expected, \
                    f"{name} should be {expected} [cache={cache_name}, actual={metrics[name]}]"

        return metrics

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

        # The state each segment is checked healthy against is the same one its baseline and
        # coordinator are read from: a partitioned segment answers control.sh over the very
        # links the test just cut, so it is fetched once per segment and passed around.
        states = {segment: self.verify_segment_healthy(segment) for segment in normalized}

        baselines = {segment: {node.consistent_id for node in states[segment].baseline} for segment in normalized}

        for seg_a, seg_b in combinations(normalized, 2):
            common_nodes = baselines[seg_a] & baselines[seg_b]

            assert not common_nodes, \
                f"Segment baselines should not intersect [common={sorted(common_nodes)}, " \
                f"{_fmt_segment(seg_a)}={sorted(baselines[seg_a])}, " \
                f"{_fmt_segment(seg_b)}={sorted(baselines[seg_b])}]"

        coordinators = {}

        for segment in normalized:
            coordinator = states[segment].coordinator

            assert coordinator, \
                f"Coordinator is not found in the {_fmt_segment(segment)} segment baseline output!"

            assert coordinator.consistent_id in baselines[segment], \
                f"{_fmt_segment(segment)} coordinator should belong to its own segment baseline " \
                f"[coordinator={coordinator.consistent_id}, baseline={sorted(baselines[segment])}]"

            coordinators[_fmt_segment(segment)] = coordinator.consistent_id

        assert len(set(coordinators.values())) == len(normalized), \
            f"Every segment should have elected its own coordinator [coordinators={coordinators}]"

    def verify_segment_healthy(self, segment: Segment):
        """
        Verifies that a segment is fully alive, ACTIVE, and its baseline covers exactly
        the servers of the DCs it consists of - and nothing else.

        :return: The ClusterState the segment was verified against, so that a caller
                 asserting further on it (see :meth:`verify_segments`) needs no second
                 control.sh round-trip into a segment that may be cut off.
        """
        dcs = _as_segment(segment)

        name = _fmt_segment(dcs)

        # get(): a per-DC dict is allowed to name only the DCs it populates, and
        # verify_whole_cluster_healthy() asks about every DC the cluster spans.
        exp_alive_nodes = sum(self.srv_per_dc.get(dc, 0) for dc in dcs)
        act_alive_nodes = sum(len(svc.alive_nodes) for dc in dcs for svc in self.dc_servers(dc))

        assert act_alive_nodes == exp_alive_nodes, \
            f"{exp_alive_nodes} nodes should be alive in {name}! [actual={act_alive_nodes}]"

        cluster_state = self.control(dcs[0]).cluster_state()

        assert "ACTIVE" == cluster_state.state, \
            f"{name} segment state should remain ACTIVE [actual={cluster_state.state}]"

        assert len(cluster_state.baseline) == exp_alive_nodes, \
            f"{name} segment baseline is not expected " \
            f"[exp={exp_alive_nodes}, actual_baseline={cluster_state.baseline}]"

        return cluster_state

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
            for svc in self.all_servers():
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


def cross_dc_network(logger, mdc: MdcCluster, delay_ms: Optional[int] = None,
                     loss: Optional[float] = None) -> NetworkGroupManager:
    """
    Builds a :class:`NetworkGroupManager` (context manager) for the cluster, applying the
    same impairment to every DC pair. With no impairments the manager still owns partition
    enable/disable and the final network cleanup.

    A cluster whose links are not all alike needs no fixture support: build the
    :class:`NetworkGroupStore` and construct the manager directly, the registry is all it
    takes from here - ``NetworkGroupManager(logger, store, mdc.network_registry())``.

    :param delay_ms: One-way cross-DC latency in milliseconds (the effective RTT is twice
           that, since netem delay is applied on egress in both directions).
    :param loss: Cross-DC packet loss fraction in [0.0, 1.0].
    """
    cfg = CrossNetworkGroupConfiguration(delay=f"{delay_ms}ms" if delay_ms is not None else None, loss=loss)

    store = NetworkGroupStore()

    if not cfg.is_empty:
        for dc_a, dc_b in all_pairs(mdc.dcs):
            store.set_config(dc_a, dc_b, cfg)

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

    assert_spread(distribution, set(expected_dcs), dc_of, owning_only, copies_per_dc, label="DC",
                  layout_hint=f"DC attribute: {dc_attr}, expected DCs: {sorted(expected_dcs)}")


def assert_spread(distribution, expected_groups, group_of, owning_only, copies_per_group, label,
                  layout_hint):
    """
    Asserts that every partition of every cache group has a copy in every expected group,
    where a group is whatever ``group_of(copy)`` returns.

    Public because the grouping is the only thing that varies: the DC spread above is one
    ``group_of``, and an affinity backup filter that spreads copies by something finer - a
    cell, an availability zone, a tuple of node attributes - is another. Such a check reuses
    this rather than walking the distribution again.

    :param distribution: CacheDistribution requested with the attributes ``group_of`` reads.
    :param expected_groups: Group keys that must each own a copy of every partition.
    :param group_of: Copy -> its group key.
    :param owning_only: Count only copies in OWNING state as present.
    :param copies_per_group: If set, each group must hold exactly this many copies.
    :param label: What a group is called in the assertion message, e.g. "DC".
    :param layout_hint: Line appended to the message, naming the layout that was expected.
    """
    violations = []

    for group in distribution.groups.values():
        for part, copies in sorted(group.partitions.items()):
            counted = [c for c in copies if not owning_only or c.state == "OWNING"]

            per_group = {key: 0 for key in expected_groups}

            for copy in counted:
                key = group_of(copy)

                if key in per_group:
                    per_group[key] += 1

            missing = {key for key, cnt in per_group.items() if cnt == 0}

            unbalanced = {} if copies_per_group is None else \
                {key: cnt for key, cnt in per_group.items() if cnt != copies_per_group}

            if missing or unbalanced:
                copies_dump = ", ".join(
                    f"{c.node_id}({'P' if c.primary else 'B'},{c.state},{label}={group_of(c)},"
                    f"{c.node_addresses})"
                    for c in copies)

                problems = []

                if missing:
                    problems.append(f"missing {label}s={sorted(missing)}")

                if unbalanced:
                    problems.append(f"copies per {label} != {copies_per_group}: {unbalanced}")

                violations.append(f"group={group.name}(id={group.group_id}), partition={part}, "
                                  f"{', '.join(problems)}, copies=[{copies_dump}]")

    assert not violations, \
        f"Partition distribution does not cover every {label}:\n  " + "\n  ".join(violations) + \
        "\n" + layout_hint
