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

package org.apache.ignite.internal.processors.cache.jta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.jetbrains.annotations.Nullable;

/**
 * Provides possibility to integrate cache transactions with JTA.
 */
public abstract class CacheJtaManagerAdapter extends GridCacheSharedManagerAdapter {
    /**
     * Checks if cache is working in JTA transaction and enlist cache as XAResource if necessary.
     *
     * @throws IgniteCheckedException In case of error.
     */
    public abstract void checkJta() throws IgniteCheckedException;

    /**
     * @param cfg Cache configuration.
     * @throws IgniteCheckedException If {@link CacheConfiguration#getTransactionManagerLookupClassName()} is incompatible with
     *     another caches or {@link TransactionConfiguration#getTxManagerLookupClassName()}.
     */
    public abstract void registerCache(CacheConfiguration<?, ?> cfg) throws IgniteCheckedException;

    /**
     * Gets transaction manager finder. Returns Object to avoid dependency on JTA library.
     *
     * @return Transaction manager finder.
     */
    @Nullable public abstract Object tmLookup();
}