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

package org.apache.ignite.internal.visor.tx;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionState;

/**
 */
public class VisorTxInfo implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid xid;

    /** */
    private final long duration;

    /** */
    private final TransactionIsolation isolation;

    /** */
    private final TransactionConcurrency concurrency;

    /** */
    private final long timeout;

    /** */
    private final String lb;

    /** */
    private final Collection<UUID> primaryNodes;

    /** */
    private final TransactionState state;

    /** */
    private int size;

    /**
     * @param xid Xid.
     * @param duration Duration.
     * @param isolation Isolation.
     * @param concurrency Concurrency.
     * @param timeout Timeout.
     * @param lb Label.
     * @param primaryNodes Primary nodes.
     * @param state State.
     * @param size Size.
     */
    public VisorTxInfo(IgniteUuid xid, long duration, TransactionIsolation isolation,
        TransactionConcurrency concurrency, long timeout, String lb, Collection<UUID> primaryNodes,
        TransactionState state, int size) {
        this.xid = xid;
        this.duration = duration;
        this.isolation = isolation;
        this.concurrency = concurrency;
        this.timeout = timeout;
        this.lb = lb;
        this.primaryNodes = primaryNodes;
        this.state = state;
        this.size = size;
    }

    /** */
    public IgniteUuid getXid() {
        return xid;
    }

    /** */
    public long getDuration() {
        return duration;
    }

    /** */
    public TransactionIsolation getIsolation() {
        return isolation;
    }

    /** */
    public TransactionConcurrency getConcurrency() {
        return concurrency;
    }

    /** */
    public long getTimeout() {
        return timeout;
    }

    /** */
    public String getLabel() {
        return lb;
    }

    /** */
    public Collection<UUID> getPrimaryNodes() {
        return primaryNodes;
    }

    /** */
    public TransactionState getState() {
        return state;
    }

    /** */
    public int getSize() {
        return size;
    }
}
