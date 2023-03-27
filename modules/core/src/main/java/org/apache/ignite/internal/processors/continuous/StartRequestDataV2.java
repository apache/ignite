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

import java.io.Serializable;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Start request data.
 */
class StartRequestDataV2 implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Serialized node filter. */
    private byte[] nodeFilterBytes;

    /** Deployment class name. */
    private String clsName;

    /** Deployment info. */
    private GridDeploymentInfo depInfo;

    /** Serialized handler. */
    private byte[] hndBytes;

    /** Buffer size. */
    private int bufSize;

    /** Time interval. */
    private long interval;

    /** Automatic unsubscribe flag. */
    private boolean autoUnsubscribe;

    /**
     * @param nodeFilterBytes Serialized node filter.
     * @param hndBytes Serialized handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     */
    StartRequestDataV2(
        byte[] nodeFilterBytes,
        byte[] hndBytes,
        int bufSize,
        long interval,
        boolean autoUnsubscribe) {
        assert hndBytes != null;
        assert bufSize > 0;
        assert interval >= 0;

        this.nodeFilterBytes = nodeFilterBytes;
        this.hndBytes = hndBytes;
        this.bufSize = bufSize;
        this.interval = interval;
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /**
     * @return Serialized node filter.
     */
    public byte[] nodeFilterBytes() {
        return nodeFilterBytes;
    }

    /**
     * @return Deployment class name.
     */
    public String className() {
        return clsName;
    }

    /**
     * @param clsName New deployment class name.
     */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /**
     * @return Deployment info.
     */
    public GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    /**
     * @param depInfo New deployment info.
     */
    public void deploymentInfo(GridDeploymentInfo depInfo) {
        this.depInfo = depInfo;
    }

    /**
     * @return Handler.
     */
    public byte[] handlerBytes() {
        return hndBytes;
    }

    /**
     * @return Buffer size.
     */
    public int bufferSize() {
        return bufSize;
    }

    /**
     * @param bufSize New buffer size.
     */
    public void bufferSize(int bufSize) {
        this.bufSize = bufSize;
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
        return S.toString(StartRequestDataV2.class, this);
    }
}
