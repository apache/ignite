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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.HashMap;
import java.util.Map;
import org.apache.curator.test.InstanceSpec;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

/**
 * Base class for Zookeeper SPI discovery tests in this package. It is intended to provide common overrides for
 * superclass methods to be shared by all subclasses.
 */
class ZookeeperDiscoverySpiSslTestBase extends ZookeeperDiscoverySpiTestBase {
    /** Zookeeper secure client port property name. */
    public static final String ZK_SECURE_CLIENT_PORT = "secureClientPort";

    /** */
    protected boolean sslEnabled;

    /** {@inheritDoc} */
    @Override protected Map<String, Object>[] clusterCustomProperties() {
        Map<String, Object>[] customProps = super.clusterCustomProperties();

        for (int i = 0; i < ZK_SRVS; i++) {
            Map<String, Object> props = customProps[i];

            if (props == null)
                props = new HashMap<>();

            props.put(ZK_SECURE_CLIENT_PORT, String.valueOf(2281 + i));

            customProps[i] = props;
        }

        return customProps;
    }

    /**
     * @return Zookeeper cluster connection string
     */
    @Override protected String getTestClusterZkConnectionString() {
        if (sslEnabled)
            return getSslConnectString();

        return super.getTestClusterZkConnectionString();
    }

    /**
     * @return Zookeeper cluster connection string
     */
    @Override protected String getRealClusterZkConnectionString() {
        if (sslEnabled)
            return "localhost:2281";

        return super.getRealClusterZkConnectionString();
    }

    /** {@inheritDoc} */
    @Override protected DiscoverySpi cloneDiscoverySpi(DiscoverySpi discoverySpi) throws Exception {
        ZookeeperDiscoverySpi clone = (ZookeeperDiscoverySpi) super.cloneDiscoverySpi(discoverySpi);

        if (USE_TEST_CLUSTER)
            clone.setZkConnectionString(getTestClusterZkConnectionString());
        else
            clone.setZkConnectionString(getRealClusterZkConnectionString());

        return clone;
    }

    /**
     * Returns the connection string to pass to the ZooKeeper constructor
     *
     * @return connection string
     */
    protected String getSslConnectString() {
        StringBuilder str = new StringBuilder();

        for (InstanceSpec spec : zkCluster.getInstances()) {
            if (str.length() > 0)
                str.append(",");

            Object secClientPort = spec.getCustomProperties().get(ZK_SECURE_CLIENT_PORT);

            if (secClientPort == null)
                throw new IllegalArgumentException("Security client port is not configured. [spec=" + spec + ']');

            str.append(spec.getHostname()).append(":").append(secClientPort);
        }

        return str.toString();
    }
}
