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

package org.apache.ignite.spi.discovery.tcp.ipfinder.zk.curator;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.test.ByteCodeRewrite;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.QuorumConfigBuilder;
import org.apache.zookeeper.ZooKeeper;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Manages an internally running ensemble of ZooKeeper servers. FOR TESTING PURPOSES ONLY.
 * This class is a copy of {{org.apache.curator.test.TestingCluster}},
 * but have very small change, that allow to run testing cluster with ZooKeeper 2.4.13 ver.
 */
public class TestingCluster implements Closeable {
    static {
        ByteCodeRewrite.apply();
    }

    /** Servers. */
    private final List<TestingZooKeeperServer> servers;

    /**
     * Creates an ensemble comprised of <code>n</code> servers. Each server will use
     * a temp directory and random ports
     *
     * @param instanceQty number of servers to create in the ensemble
     */
    public TestingCluster(int instanceQty) {
        this(makeSpecs(instanceQty));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs the server specs
     */
    public TestingCluster(InstanceSpec... specs) {
        this(listToMap(ImmutableList.copyOf(specs)));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs the server specs
     */
    public TestingCluster(Collection<InstanceSpec> specs) {
        this(listToMap(specs));
    }

    /**
     * Creates an ensemble using the given server specs
     *
     * @param specs map of an instance spec to its set of quorum instances. Allows simulation of an ensemble with
     * instances having different config peers
     */
    public TestingCluster(Map<InstanceSpec, Collection<InstanceSpec>> specs) {
        ImmutableList.Builder<TestingZooKeeperServer> serverBuilder = ImmutableList.builder();
        for (Map.Entry<InstanceSpec, Collection<InstanceSpec>> entry : specs.entrySet()) {
            List<InstanceSpec> instanceSpecs = Lists.newArrayList(entry.getValue());
            int index = instanceSpecs.indexOf(entry.getKey());
            Preconditions.checkState(index >= 0, entry.getKey() + " not found in specs");
            QuorumConfigBuilder builder = new QuorumConfigBuilder(instanceSpecs);
            serverBuilder.add(new TestingZooKeeperServer(builder, index));
        }
        servers = serverBuilder.build();
    }

    /**
     * Returns the set of servers in the ensemble
     *
     * @return set of servers
     */
    public Collection<InstanceSpec> getInstances() {
        Iterable<InstanceSpec> transformed = Iterables.transform
            (
                servers,
                new Function<TestingZooKeeperServer, InstanceSpec>() {
                    @Override
                    public InstanceSpec apply(TestingZooKeeperServer server) {
                        return server.getInstanceSpec();
                    }
                }
            );
        return Lists.newArrayList(transformed);
    }

    public List<TestingZooKeeperServer> getServers() {
        return Lists.newArrayList(servers);
    }

    /**
     * Returns the connection string to pass to the ZooKeeper constructor
     *
     * @return connection string
     */
    public String getConnectString() {
        StringBuilder str = new StringBuilder();
        for (InstanceSpec spec : getInstances()) {
            if (str.length() > 0)
                str.append(",");

            str.append(spec.getConnectString());
        }
        return str.toString();
    }

    /**
     * Start the ensemble. The cluster must be started before use.
     *
     * @throws Exception errors
     */
    public void start() throws Exception {
        for (TestingZooKeeperServer server : servers)
            server.start();

    }

    /**
     * Shutdown the ensemble WITHOUT freeing resources, etc.
     */
    public void stop() throws IOException {
        for (TestingZooKeeperServer server : servers)
            server.stop();

    }

    /**
     * Shutdown the ensemble, free resources, etc. If temp directories were used, they
     * are deleted. You should call this in a <code>finally</code> block.
     *
     * @throws IOException errors
     */
    @Override public void close() throws IOException {
        for (TestingZooKeeperServer server : servers)
            server.close();
    }

    /**
     * Kills the given server. This simulates the server unexpectedly crashing
     *
     * @param instance server to kill
     * @return true if the instance was found
     * @throws Exception errors
     */
    public boolean killServer(InstanceSpec instance) throws Exception {
        for (TestingZooKeeperServer server : servers) {
            if (server.getInstanceSpec().equals(instance)) {
                server.kill();
                return true;
            }
        }
        return false;
    }

    /**
     * Restart the given server of the cluster
     *
     * @param instance server instance
     * @return true of the server was found
     * @throws Exception errors
     */
    public boolean restartServer(InstanceSpec instance) throws Exception {
        for (TestingZooKeeperServer server : servers) {
            if (server.getInstanceSpec().equals(instance)) {
                server.restart();
                return true;
            }
        }
        return false;
    }

    /**
     * Given a ZooKeeper instance, returns which server it is connected to
     *
     * @param client ZK instance
     * @return the server
     * @throws Exception errors
     */
    public InstanceSpec findConnectionInstance(ZooKeeper client) throws Exception {
        Method m = client.getClass().getDeclaredMethod("testableRemoteSocketAddress");
        m.setAccessible(true);
        InetSocketAddress address = (InetSocketAddress)m.invoke(client);
        if (address != null) {
            for (TestingZooKeeperServer server : servers) {
                if (server.getInstanceSpec().getPort() == address.getPort())
                    return server.getInstanceSpec();
            }
        }

        return null;
    }

    private static Map<InstanceSpec, Collection<InstanceSpec>> makeSpecs(int instanceQty) {
        ImmutableList.Builder<InstanceSpec> builder = ImmutableList.builder();
        for (int i = 0; i < instanceQty; ++i)
            builder.add(InstanceSpec.newInstanceSpec());

        return listToMap(builder.build());
    }

    private static Map<InstanceSpec, Collection<InstanceSpec>> listToMap(Collection<InstanceSpec> list) {
        ImmutableMap.Builder<InstanceSpec, Collection<InstanceSpec>> mapBuilder = ImmutableMap.builder();
        for (InstanceSpec spec : list)
            mapBuilder.put(spec, list);

        return mapBuilder.build();
    }
}
