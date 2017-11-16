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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 *
 */
public class ZkDiscoveryImpl {
    /** */
    private static final String IGNITE_PATH = "/apache-ignite";

    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final IgniteLogger log;

    /** */
    private final DiscoverySpiListener lsnr;

    /** */
    private ZookeeperClient zkClient;

    /** */
    private final GridFutureAdapter<Void> joinFut = new GridFutureAdapter<>();

    public ZkDiscoveryImpl(IgniteLogger log, DiscoverySpiListener lsnr) {
        this.log = log.getLogger(getClass());
        this.lsnr = lsnr;
    }

    public void joinTopology(String igniteInstanceName, String connectString, int sesTimeout)
        throws InterruptedException {
        try {
            zkClient = new ZookeeperClient(igniteInstanceName,
                log,
                connectString,
                sesTimeout,
                new ConnectionLossListener());
        }
        catch (Exception e) {
            throw new IgniteSpiException("Failed to create Zookeeper client", e);
        }

        try {
            zkClient.createIfNeeded(IGNITE_PATH, null, CreateMode.PERSISTENT);
        }
        catch (ZookeeperClientFailedException e) {
            throw new IgniteSpiException("Failed to initialize Zookeeper nodes", e);
        }
    }

    /**
     *
     */
    public void stop() {
        if (zkClient != null)
            zkClient.close();
    }

    private <T> T unmarshal(byte[] data) throws IgniteCheckedException {
        return marsh.unmarshal(data, null);
    }

    private byte[] marshal(Object obj) throws IgniteCheckedException {
        return marsh.marshal(obj);
    }

    /**
     *
     */
    private class ConnectionLossListener implements IgniteRunnable {
        @Override public void run() {

        }
    }

    /**
     *
     */
    private class ZKChildrenUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

        }
    }
}
