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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Message sent to check that transactions related to transaction were prepared on remote node.
 */
public class GridCacheTxRecoveryRequest extends GridDistributedBaseMessage {
    /** Future ID. */
    @Order(value = 7, method = "futureId")
    private IgniteUuid futId;

    /** Mini future ID. */
    @Order(8)
    private IgniteUuid miniId;

    /** Near transaction ID. */
    @Order(value = 9, method = "nearXidVersion")
    private GridCacheVersion nearXidVer;

    /** Expected number of transactions on node. */
    @Order(value = 10, method = "transactions")
    private int txNum;

    /** System transaction flag. */
    @Order(value = 11, method = "system")
    private boolean sys;

    /** {@code True} if should check only tx on near node. */
    @Order(12)
    private boolean nearTxCheck;

    /**
     * Empty constructor.
     */
    public GridCacheTxRecoveryRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param txNum Expected number of transactions on remote node.
     * @param nearTxCheck {@code True} if should check only tx on near node.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheTxRecoveryRequest(IgniteInternalTx tx,
        int txNum,
        boolean nearTxCheck,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean addDepInfo
    ) {
        super(tx.xidVersion(), 0, addDepInfo);

        nearXidVer = tx.nearXidVersion();
        sys = tx.system();

        this.futId = futId;
        this.miniId = miniId;
        this.txNum = txNum;
        this.nearTxCheck = nearTxCheck;
    }

    /**
     * @return {@code True} if should check only tx on near node.
     */
    public boolean nearTxCheck() {
        return nearTxCheck;
    }

    /**
     * @param nearTxCheck {@code True} if should check only tx on near node.
     */
    public void nearTxCheck(boolean nearTxCheck) {
        this.nearTxCheck = nearTxCheck;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @param nearXidVer Near version.
     */
    public void nearXidVersion(GridCacheVersion nearXidVer) {
        this.nearXidVer = nearXidVer;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Expected number of transactions on node.
     */
    public int transactions() {
        return txNum;
    }

    /**
     * @param txNum Expected number of transactions on node.
     */
    public void transactions(int txNum) {
        this.txNum = txNum;
    }

    /**
     * @return System transaction flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @param sys System transaction flag.
     */
    public void system(boolean sys) {
        this.sys = sys;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txRecoveryMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 16;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryRequest.class, this, "super", super.toString());
    }
}
