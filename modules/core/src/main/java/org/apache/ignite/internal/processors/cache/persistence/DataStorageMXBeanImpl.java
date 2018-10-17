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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.DataStorageMXBean;

/**
 * TransactionsMXBean implementation.
 */
public class DataStorageMXBeanImpl implements DataStorageMXBean {
    /** */
    private final GridKernalContextImpl ctx;

    /**
     * @param ctx Context.
     */
    public DataStorageMXBeanImpl(GridKernalContextImpl ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public int getWalCompactionLevel() {
        return ctx.config().getDataStorageConfiguration().getWalCompactionLevel();
    }

    /** {@inheritDoc} */
    @Override public void setWalCompactionLevel(int walCompactionLevel) {
        ctx.config().getDataStorageConfiguration().setWalCompactionLevel(walCompactionLevel);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataStorageMXBeanImpl.class, this);
    }
}
