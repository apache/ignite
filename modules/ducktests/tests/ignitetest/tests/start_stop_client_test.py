
from ducktape.mark.resource import cluster
import time

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_app import IgniteApplicationService
from ignitetest.services.spark import SparkService
from ignitetest.services.utils.ignite_configuration.discovery import from_ignite_cluster
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.zk.zookeeper import ZookeeperService
from ignitetest.utils import ignite_versions
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


# pylint: disable=W0223



class StartStopClientTest(IgniteTest):
    """
    CACHE_NAME - name of the cache to create for the test
    REPORT_NAME - the name of the tests
    PACING - the frequency of the operation on clients (ms)
    JAVA_CLIENT_CLASS_NAME - running classname
    CLIENTS_WORK_TIME_S - clients working time (s)
    ITERATION_COUNT - the number of iterations of starting and stopping client nodes (s)
    CLUSTER_NODES - cluster size
    STATIC_CLIENTS_NUM - the number of permanently employed clients
    TEMP_CLIENTS_NUM - number of clients who come log in and out
    """


    CACHE_NAME = "simple-tx-cache"
    REPORT_NAME = "put-tx"
    PACING = 10
    JAVA_CLIENT_CLASS_NAME = "org.apache.ignite.internal.ducktest.tests.start_stop_client.SingleClientNode"

    CLIENTS_WORK_TIME_S=30
    ITERATION_COUNT=2
    CLUSTER_NODES=12
    STATIC_CLIENTS_NUM = 2
    TEMP_CLIENTS_NUM = 7

    @cluster(num_nodes=CLUSTER_NODES)
    @ignite_versions(str(DEV_BRANCH))
    def test_ignite_start_stop(self, ignite_version):


        servers_count=self.CLUSTER_NODES-self.STATIC_CLIENTS_NUM-self.TEMP_CLIENTS_NUM
        end_test_topology_version = 2*(servers_count + self.STATIC_CLIENTS_NUM)+(2*self.ITERATION_COUNT*self.TEMP_CLIENTS_NUM)-1
        topology_ver=servers_count

        server_configuration = IgniteConfiguration(version=IgniteVersion(ignite_version))
        active_clients_count = self.STATIC_CLIENTS_NUM+self.TEMP_CLIENTS_NUM

        ignite = IgniteService(self.test_context, server_configuration, num_nodes=topology_ver)
        print(self.test_context)
        ignite.start()
        ignite.await_event("servers=" + str(topology_ver),
                           timeout_sec=60,
                           from_the_beginning=True,
                           backoff_sec=1)

        client_configuration = server_configuration._replace(client_mode=True, discovery_spi=from_ignite_cluster(ignite))

        #static clients
        static_cl = []
        #temporary clients
        temp_cl = []
        #init client that work throughout the entire launch
        for s_num in range(self.STATIC_CLIENTS_NUM):
            static_cl.append(clientCfg(self.test_context, client_configuration,
                                       self.JAVA_CLIENT_CLASS_NAME,
                                       self.REPORT_NAME,
                                       self.PACING,
                                       self.CACHE_NAME))

        #clients who work for a limited time interval
        for t_num in range(self.TEMP_CLIENTS_NUM):
            temp_cl.append(clientCfg(self.test_context, client_configuration,
                                     self.JAVA_CLIENT_CLASS_NAME,
                                     self.REPORT_NAME,
                                     self.PACING,
                                     self.CACHE_NAME))

        topology_ver = startClients(static_cl, ignite)
        checkClusterState(ignite, servers_count, self.STATIC_CLIENTS_NUM)

        for iteration_number in range(self.ITERATION_COUNT):
            topology_ver = startClients(temp_cl, ignite)
            checkClusterState(ignite, servers_count, active_clients_count)
            time.sleep(self.CLIENTS_WORK_TIME_S)
            topology_ver = stopClients(temp_cl, ignite)
            checkClusterState(ignite, servers_count, self.STATIC_CLIENTS_NUM)

        topology_ver = stopClients(static_cl, ignite)
        checkClusterState(ignite, servers_count, 0)
        time.sleep(10)
        ignite.stop()

def clientCfg(test_context, client_configuration, java_classname, report_name, pacing, cacheName):
    app = IgniteApplicationService(
        test_context,
        client_configuration,
        java_class_name=java_classname,
        params={"cacheName": cacheName,
                "reportName": report_name,
                "pacing": pacing})
    return app

def startClients(static_cl, ignite):
    for client in static_cl:
        print("start ignite client.")
        client.start()

def stopClients(static_cl, ignite):
    for client in static_cl:
        print("stop ignite client.")
        client.stop()

def checkClusterState(ignite, servers_count, clients_count):

    ignite.await_event("servers=" + str(servers_count),
                       timeout_sec=60,
                       from_the_beginning=False,
                       backoff_sec=1)
    ignite.await_event("clients=" + str(clients_count),
                       timeout_sec=60,
                       from_the_beginning=False,
                       backoff_sec=1)
