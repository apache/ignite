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

package org.apache.ignite.internal.cluster;

import org.apache.ignite.cluster.ClusterStartNodeResult;

/**
 * Implementation for cluster start node result. 
 */
public class ClusterStartNodeResultImpl implements ClusterStartNodeResult {
    /** Host name. */
    private String hostName;

    /** Result (success or failure). */
    private boolean success;

    /** Error message (if any) */
    private String error;

    /**
     * @param hostName Host name.
     * @param success Success or not.
     * @param error Error message.
     */
    public ClusterStartNodeResultImpl(String hostName, boolean success, String error) {
        this.hostName = hostName;
        this.success = success;
        this.error = error;
    }

    /** {@inheritDoc} */
    @Override public String getHostName() {
        return hostName;
    }

    /**
     * Sets host name.
     *
     * @param hostName Host name.
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /** {@inheritDoc} */
    @Override public boolean isSuccess() {
        return success;
    }

    /**
     * Sets success result.
     *
     * @param success Success result.
     */
    public void setSuccess(boolean success) {
        this.success = success;
    }

    /** {@inheritDoc} */
    @Override public String getError() {
        return error;
    }

    /**
     * Sets error message.
     *
     * @param error Error message.
     */
    public void setError(String error) {
        this.error = error;
    }
}