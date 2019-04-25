/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Closure that is sent on all server nodes in order to change transaction configuration parameter
 * that allows or disallows dump requests from local to near nodes while detection long running
 * transactions.
 */
public class TxOwnerDumpRequestAllowedSettingClosure implements IgniteRunnable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final boolean allowed;

    /**
     * Auto-inject Ignite instance
     */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    public TxOwnerDumpRequestAllowedSettingClosure(boolean allowed) {
        this.allowed = allowed;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        ((IgniteEx)ignite)
            .context()
            .cache()
            .context()
            .tm()
            .setTxOwnerDumpRequestsAllowed(allowed);
    }
}
