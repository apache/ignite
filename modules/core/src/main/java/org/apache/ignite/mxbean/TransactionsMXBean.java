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

package org.apache.ignite.mxbean;

/**
 * Transactions MXBean interface.
 */
@MXBeanDescription("MBean that provides access to Ignite transactions.")
public interface TransactionsMXBean {
    /**
     * Shows if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     *
     * @return <code>true</code> if allowed, <code>false</code> otherwise.
     */
    @MXBeanDescription(
        "Shows if dump requests from local node to near node are allowed, " +
        "when long running transaction  is found. If allowed, the compute request to near " +
        "node will be made to get thread dump of transaction owner thread."
    )
    public boolean getTxOwnerDumpRequestsAllowed();

    /**
     * Sets if dump requests from local node to near node are allowed, when long running transaction
     * is found. If allowed, the compute request to near node will be made to get thread dump of transaction
     * owner thread.
     *
     * @param allowed whether to allow
     */
    @MXBeanDescription(
        "Shows if dump requests from local node to near node are allowed, " +
        "when long running transaction  is found. If allowed, the compute request to near " +
        "node will be made to get thread dump of transaction owner thread."
    )
    @MXBeanParametersNames("allowed")
    @MXBeanParametersDescriptions(
        "whether to allow"
    )
    public void setTxOwnerDumpRequestsAllowed(boolean allowed);
}
