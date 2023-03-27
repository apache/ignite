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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * The task for changing transaction timeout on partition map exchange.
 */
public class TxTimeoutOnPartitionMapExchangeChangeTask extends AbstractCachePartitionExchangeWorkerTask {
    /** Discovery message. */
    private final TxTimeoutOnPartitionMapExchangeChangeMessage msg;

    /**
     * Constructor.
     *
     * @param secCtx Security context in which current task must be executed.
     * @param msg Discovery message.
     */
    public TxTimeoutOnPartitionMapExchangeChangeTask(SecurityContext secCtx, TxTimeoutOnPartitionMapExchangeChangeMessage msg) {
        super(secCtx);

        assert msg != null;
        this.msg = msg;
    }

    /**
     * Gets discovery message.
     *
     * @return Discovery message.
     */
    public TxTimeoutOnPartitionMapExchangeChangeMessage message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TxTimeoutOnPartitionMapExchangeChangeTask.class, this);
    }
}
