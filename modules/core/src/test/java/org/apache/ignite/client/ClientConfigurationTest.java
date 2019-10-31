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

package org.apache.ignite.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertTrue;

/**
 * {@link ClientConfiguration} unit tests.
 */
public class ClientConfigurationTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

    /** Serialization/deserialization. */
    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        ClientConfiguration target = new ClientConfiguration()
            .setAddresses("127.0.0.1:10800", "127.0.0.1:10801")
            .setTimeout(123)
            .setBinaryConfiguration(new BinaryConfiguration()
                .setClassNames(Collections.singleton("Person"))
            )
            .setSslMode(SslMode.REQUIRED)
            .setSslClientCertificateKeyStorePath("client.jks")
            .setSslClientCertificateKeyStoreType("JKS")
            .setSslClientCertificateKeyStorePassword("123456")
            .setSslTrustCertificateKeyStorePath("trust.jks")
            .setSslTrustCertificateKeyStoreType("JKS")
            .setSslTrustCertificateKeyStorePassword("123456")
            .setSslKeyAlgorithm("SunX509");

        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();

        ObjectOutput out = new ObjectOutputStream(outBytes);

        out.writeObject(target);
        out.flush();

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(outBytes.toByteArray()));

        Object desTarget = in.readObject();

        assertTrue(Comparers.equal(target, desTarget));
    }

    /**
     * Test check the case when {@link IgniteConfiguration#getRebalanceThreadPoolSize()} is equal to {@link
     * IgniteConfiguration#getSystemThreadPoolSize()}
     */
    @Test
    public void testRebalanceThreadPoolSize() {
        GridStringLogger gridStrLog = new GridStringLogger();
        gridStrLog.logLength(1024 * 100);

        IgniteConfiguration cci = Config.getServerConfiguration().setClientMode(true);
        cci.setRebalanceThreadPoolSize(cci.getSystemThreadPoolSize());
        cci.setGridLogger(gridStrLog);

        try (
            Ignite si = Ignition.start(Config.getServerConfiguration());
            Ignite ci = Ignition.start(cci)) {
            Set<ClusterNode> collect = si.cluster().nodes().stream()
                .filter(new Predicate<ClusterNode>() {
                    @Override public boolean test(ClusterNode clusterNode) {
                        return clusterNode.isClient();
                    }
                })
                .collect(Collectors.toSet());

            String log = gridStrLog.toString();
            boolean containsMsg = log.contains("Setting the rebalance pool size has no effect on the client mode");

            Assert.assertTrue(containsMsg);
            Assert.assertEquals(1, collect.size());
        }
    }
}
