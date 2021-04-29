/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.snippets;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientAuthenticationException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.client.SslProtocol;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ClientTransactionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.jupiter.api.Test;

public class JavaThinClient {

    public static void main(String[] args) throws ClientException, Exception {
        JavaThinClient test = new JavaThinClient();

        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        try (IgniteClient client = Ignition.startClient(cfg)) {
            test.scanQuery(client);
        }
    }

    @Test
    void clusterConnection() {
        // tag::clusterConfiguration[]
        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration();
        // Set a port range from 10000 to 10005
        clientConnectorCfg.setPort(10000);
        clientConnectorCfg.setPortRange(5);

        IgniteConfiguration cfg = new IgniteConfiguration().setClientConnectorConfiguration(clientConnectorCfg);

        // Start a node
        Ignite ignite = Ignition.start(cfg);
        // end::clusterConfiguration[]

        ignite.close();
    }

    void tx(IgniteClient client) {
        //tag::tx[]
        ClientCache<Integer, String> cache = client.cache("my_transactional_cache");

        ClientTransactions tx = client.transactions();

        try (ClientTransaction t = tx.txStart()) {
            cache.put(1, "new value");

            t.commit();
        }
        //end::tx[]
    }

    @Test
    void transactionConfiguration() {

        // tag::transaction-config[]
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses("localhost:10800");

        cfg.setTransactionConfiguration(new ClientTransactionConfiguration().setDefaultTxTimeout(10000)
                .setDefaultTxConcurrency(TransactionConcurrency.OPTIMISTIC)
                .setDefaultTxIsolation(TransactionIsolation.REPEATABLE_READ));

        IgniteClient client = Ignition.startClient(cfg);

        // end::transaction-config[]

        ClientCache cache = client.createCache(
                new ClientCacheConfiguration().setName("test").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        // tag::tx-custom-properties[]
        ClientTransactions tx = client.transactions();
        try (ClientTransaction t = tx.txStart(TransactionConcurrency.OPTIMISTIC,
                TransactionIsolation.REPEATABLE_READ)) {
            cache.put(1, "new value");
            t.commit();
        }
        // end::tx-custom-properties[]

    }

    void connection() throws ClientException, Exception {

        // tag::clientConnection[]
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        try (IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Integer, String> cache = client.cache("myCache");
            // Get data from the cache
        }
        // end::clientConnection[]
    }

    void connectionToMultipleNodes() throws ClientException, Exception {
        // tag::connect-to-many-nodes[]
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("node1_address:10800",
                "node2_address:10800", "node3_address:10800"))) {
        } catch (ClientConnectionException ex) {
            // All the servers are unavailable
        }
        // end::connect-to-many-nodes[]
    }

    ClientCache<Integer, String> createCache(IgniteClient client) {
        // tag::getOrCreateCache[]
        ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName("References")
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ClientCache<Integer, String> cache = client.getOrCreateCache(cacheCfg);
        // end::getOrCreateCache[]
        return cache;
    }

    void keyValueOperations(ClientCache<Integer, String> cache) {
        // tag::key-value-operations[]
        Map<Integer, String> data = IntStream.rangeClosed(1, 100).boxed()
                .collect(Collectors.toMap(i -> i, Object::toString));

        cache.putAll(data);

        assert !cache.replace(1, "2", "3");
        assert "1".equals(cache.get(1));
        assert cache.replace(1, "1", "3");
        assert "3".equals(cache.get(1));

        cache.put(101, "101");

        cache.removeAll(data.keySet());
        assert cache.size() == 1;
        assert "101".equals(cache.get(101));

        cache.removeAll();
        assert 0 == cache.size();
        // end::key-value-operations[]
        System.out.println("done");
    }

    void scanQuery(IgniteClient client) {

        // tag::scan-query[]
        ClientCache<Integer, Person> personCache = client.getOrCreateCache("personCache");

        Query<Cache.Entry<Integer, Person>> qry = new ScanQuery<Integer, Person>(
                (i, p) -> p.getName().contains("Smith"));

        try (QueryCursor<Cache.Entry<Integer, Person>> cur = personCache.query(qry)) {
            for (Cache.Entry<Integer, Person> entry : cur) {
                // Process the entry ...
            }
        }
        // end::scan-query[]
    }

    void binary(IgniteClient client) {
        // tag::binary-example[]
        IgniteBinary binary = client.binary();

        BinaryObject val = binary.builder("Person").setField("id", 1, int.class).setField("name", "Joe", String.class)
                .build();

        ClientCache<Integer, BinaryObject> cache = client.cache("persons").withKeepBinary();

        cache.put(1, val);

        BinaryObject value = cache.get(1);
        // end::binary-example[]
    }

    void sql(IgniteClient client) {

        // tag::sql[]
        client.query(new SqlFieldsQuery(String.format(
                "CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR) WITH \"VALUE_TYPE=%s\"",
                Person.class.getName())).setSchema("PUBLIC")).getAll();

        int key = 1;
        Person val = new Person(key, "Person 1");

        client.query(new SqlFieldsQuery("INSERT INTO Person(id, name) VALUES(?, ?)").setArgs(val.getId(), val.getName())
                .setSchema("PUBLIC")).getAll();

        FieldsQueryCursor<List<?>> cursor = client
                .query(new SqlFieldsQuery("SELECT name from Person WHERE id=?").setArgs(key).setSchema("PUBLIC"));

        // Get the results; the `getAll()` methods closes the cursor; you do not have to
        // call cursor.close();
        List<List<?>> results = cursor.getAll();

        results.stream().findFirst().ifPresent(columns -> {
            System.out.println("name = " + columns.get(0));
        });
        // end::sql[]
    }

    public static final String KEYSTORE = "keystore/client.jks";
    public static final String TRUSTSTORE = "keystore/trust.jks";

    @Test
    void configureSSL() throws ClientException, Exception {

        // tag::ssl-configuration[]
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");

        clientCfg.setSslMode(SslMode.REQUIRED).setSslClientCertificateKeyStorePath(KEYSTORE)
                .setSslClientCertificateKeyStoreType("JKS").setSslClientCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStorePath(TRUSTSTORE).setSslTrustCertificateKeyStorePassword("123456")
                .setSslTrustCertificateKeyStoreType("JKS").setSslKeyAlgorithm("SunX509").setSslTrustAll(false)
                .setSslProtocol(SslProtocol.TLS);

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            // ...
        }
        // end::ssl-configuration[]
    }

    void configureSslInCluster() {
        // tag::cluster-ssl-configuration[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setSslEnabled(true);
        clientCfg.setUseIgniteSslContextFactory(false);
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStoreFilePath("/path/to/server.jks");
        sslContextFactory.setKeyStorePassword("123456".toCharArray());

        sslContextFactory.setTrustStoreFilePath("/path/to/trust.jks");
        sslContextFactory.setTrustStorePassword("123456".toCharArray());

        clientCfg.setSslContextFactory(sslContextFactory);

        igniteCfg.setClientConnectorConfiguration(clientCfg);

        // end::cluster-ssl-configuration[]
    }

    void clusterUseGlobalSllContext() {
        // tag::use-global-ssl[]
        ClientConnectorConfiguration clientConnectionCfg = new ClientConnectorConfiguration();
        clientConnectionCfg.setSslEnabled(true);
        // end::use-global-ssl[]
    }

    void clientAuthentication() throws ClientException, Exception {
        // tag::client-authentication[]
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800").setUserName("joe")
                .setUserPassword("passw0rd!");

        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            // ...
        } catch (ClientAuthenticationException e) {
            // Handle authentication failure
        }
        // end::client-authentication[]
    }

    void resultsToMap(ClientCache<Integer, Person> cache) {
        // tag::results-to-map[]
        Query<Cache.Entry<Integer, Person>> qry = new ScanQuery<Integer, Person>(
                (i, p) -> p.getName().contains("Smith"));

        try (QueryCursor<Cache.Entry<Integer, Person>> cur = cache.query(qry)) {
            // Collecting the results into a map removes the duplicates
            Map<Integer, Person> res = cur.getAll().stream()
                    .collect(Collectors.toMap(Cache.Entry::getKey, Cache.Entry::getValue));
        }
        // end::results-to-map[]
    }


    void veiwsystemview() {
        //tag::system-views[]
        ClientConfiguration cfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");

        try (IgniteClient igniteClient = Ignition.startClient(cfg)) {

            // getting the id of the first node
            UUID nodeId = (UUID) igniteClient.query(new SqlFieldsQuery("SELECT * from NODES").setSchema("IGNITE"))
            .getAll().iterator().next().get(0);

            double cpu_load = (Double) igniteClient
            .query(new SqlFieldsQuery("select CUR_CPU_LOAD * 100 from NODE_METRICS where NODE_ID = ? ")
            .setSchema("IGNITE").setArgs(nodeId.toString()))
            .getAll().iterator().next().get(0);

            System.out.println("node's cpu load = " + cpu_load);

        } catch (ClientException e) {
            System.err.println(e.getMessage());
        } catch (Exception e) {
            System.err.format("Unexpected failure: %s\n", e);
        }

        //end::system-views[]
    }

    void partitionAwareness() throws Exception {
        //tag::partition-awareness[]
        ClientConfiguration cfg = new ClientConfiguration()
                .setAddresses("node1_address:10800", "node2_address:10800", "node3_address:10800")
                .setPartitionAwarenessEnabled(true);

        try (IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Integer, String> cache = client.cache("myCache");
            // Put, get or remove data from the cache...
        } catch (ClientException e) {
            System.err.println(e.getMessage());
        }
        //end::partition-awareness[]
    }

    void clientAddressFinder() throws Exception {
        //tag::client-address-finder[]
        ClientAddressFinder finder = () -> {
            String[] dynamicServerAddresses = fetchServerAddresses();

            return dynamicServerAddresses;
        };

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddressFinder(finder)
            .setPartitionAwarenessEnabled(true);

        try (IgniteClient client = Ignition.startClient(cfg)) {
            ClientCache<Integer, String> cache = client.cache("myCache");
            // Put, get, or remove data from the cache...
        } catch (ClientException e) {
            System.err.println(e.getMessage());
        }
        //end::client-address-finder[]
    }

    @Test
    void cientCluster() throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        //tag::client-cluster[]
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            ClientCluster clientCluster = client.cluster();
            clientCluster.state(ClusterState.ACTIVE);
        }
        //end::client-cluster[]
    }

    void clientClusterGroups() throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        //tag::client-cluster-groups[]
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            ClientClusterGroup serversInDc1 = client.cluster().forServers().forAttribute("dc", "dc1");
            serversInDc1.nodes().forEach(n -> System.out.println("Node ID: " + n.id()));
        }
        //end::client-cluster-groups[]
    }

    void clientCompute() throws Exception {
        //tag::client-compute-setup[]
        ThinClientConfiguration thinClientCfg = new ThinClientConfiguration()
                .setMaxActiveComputeTasksPerConnection(100);

        ClientConnectorConfiguration clientConnectorCfg = new ClientConnectorConfiguration()
                .setThinClientConfiguration(thinClientCfg);

        IgniteConfiguration igniteCfg = new IgniteConfiguration()
                .setClientConnectorConfiguration(clientConnectorCfg);

        Ignite ignite = Ignition.start(igniteCfg);
        //end::client-compute-setup[]

        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        //tag::client-compute-task[]
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            // Suppose that the MyTask class is already deployed in the cluster
            client.compute().execute(
                MyTask.class.getName(), "argument");
        }
        //end::client-compute-task[]
    }

    void clientServices() throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        //tag::client-services[]
        try (IgniteClient client = Ignition.startClient(clientCfg)) {
            // Executing the service named MyService
            // that is already deployed in the cluster.
            client.services().serviceProxy(
                "MyService", MyService.class).myServiceMethod();
        }
        //end::client-services[]
    }

    void asyncApi() throws Exception {
        ClientConfiguration clientCfg = new ClientConfiguration().setAddresses("127.0.0.1:10800");
        //tag::async-api[]
        IgniteClient client = Ignition.startClient(clientCfg);
        ClientCache<Integer, String> cache = client.getOrCreateCache("cache");

        IgniteClientFuture<Void> putFut = cache.putAsync(1, "hello");
        putFut.get(); // Blocking wait.

        IgniteClientFuture<String> getFut = cache.getAsync(1);
        getFut.thenAccept(val -> System.out.println(val)); // Non-blocking continuation.
        //end::async-api[]
    }

    private static class MyTask {
    }

    private static interface MyService {
        public void myServiceMethod();
    }
}
