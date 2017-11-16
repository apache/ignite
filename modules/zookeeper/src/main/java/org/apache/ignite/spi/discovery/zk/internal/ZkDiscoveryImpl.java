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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.data.Stat;

/**
 *
 */
public class ZkDiscoveryImpl {
    /** */
    private final JdkMarshaller marsh = new JdkMarshaller();

    /** */
    private final IgniteLogger log;

    /** */
    private final DiscoverySpiListener lsnr;

    /** */
    private ZookeeperClient zkClient;

    public ZkDiscoveryImpl(IgniteLogger log, DiscoverySpiListener lsnr) {
        this.log = log.getLogger(getClass());
        this.lsnr = lsnr;
    }

    public void joinTopology(String igniteInstanceName) {

        //zkClient.
    }

    /**
     *
     */
    private class ZKChildrenUpdateCallback implements AsyncCallback.Children2Callback {
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {

        }
    }
}
