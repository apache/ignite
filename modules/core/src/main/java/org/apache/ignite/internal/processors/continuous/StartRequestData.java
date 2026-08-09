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

package org.apache.ignite.internal.processors.continuous;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Start request data.
 */
public class StartRequestData implements Message {
    /** Node filter, restored by the processor reading this request. */
    IgnitePredicate<ClusterNode> nodeFilter;

    /** Serialized node filter. */
    @Order(0)
    byte[] nodeFilterBytes;

    /** Deployment class name. */
    @Order(1)
    String clsName;

    /** Deployment info. */
    @Order(2)
    GridDeploymentInfoMessage depInfo;

    /** Handler, restored by the processor reading this request. */
    GridContinuousHandler hnd;

    /** Serialized handler. */
    @Order(3)
    byte[] hndBytes;

    /** Buffer size. */
    @Order(4)
    int bufSize;

    /** Time interval. */
    @Order(5)
    long interval;

    /** Automatic unsubscribe flag. */
    @Order(6)
    boolean autoUnsubscribe;

    /** Keep binary flag. */
    @Order(7)
    boolean keepBinary;

    /** */
    public StartRequestData() {}

    /**
     * @param nodeFilter Node filter.
     * @param hnd Handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     */
    public StartRequestData(
        IgnitePredicate<ClusterNode> nodeFilter,
        GridContinuousHandler hnd,
        int bufSize,
        long interval,
        boolean autoUnsubscribe,
        boolean keepBinary) {
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        this.nodeFilter = nodeFilter;
        this.hnd = hnd;
        this.bufSize = bufSize;
        this.interval = interval;
        this.autoUnsubscribe = autoUnsubscribe;
        this.keepBinary = keepBinary;
    }

    /**
     * @return Node filter.
     */
    public IgnitePredicate<ClusterNode> nodeFilter() {
        return nodeFilter;
    }

    /**
     * @param clsName New deployment class name.
     */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /**
     * @param depInfo New deployment info.
     */
    public void deploymentInfo(GridDeploymentInfoMessage depInfo) {
        this.depInfo = depInfo;
    }

    /**
     * @return Handler.
     */
    public GridContinuousHandler handler() {
        return hnd;
    }

    /**
     * @return Buffer size.
     */
    public int bufferSize() {
        return bufSize;
    }

    /**
     * @return Time interval.
     */
    public long interval() {
        return interval;
    }

    /**
     * @param interval New time interval.
     */
    public void interval(long interval) {
        this.interval = interval;
    }

    /**
     * @return Automatic unsubscribe flag.
     */
    public boolean autoUnsubscribe() {
        return autoUnsubscribe;
    }

    /**
     * @param autoUnsubscribe New automatic unsubscribe flag.
     */
    public void autoUnsubscribe(boolean autoUnsubscribe) {
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StartRequestData.class, this);
    }
}
