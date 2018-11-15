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

package org.apache.ignite.tests.utils;

import java.util.Map;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Test implementation of {@link CacheStoreSession} for the unit tests purposes
 */
public class TestCacheSession implements CacheStoreSession {
    /** */
    private String cacheName;

    /** */
    private Transaction tx;

    /** */
    private Map<Object, Object> props = U.newHashMap(1);

    /** */
    private Object attach;

    /** */
    public TestCacheSession(String cacheName) {
        this.cacheName = cacheName;
    }

    /** */
    public TestCacheSession(String cacheName, Transaction tx, Map<Object, Object> props) {
        this.cacheName = cacheName;
        this.tx = tx;
        this.props = props;
    }

    /** */
    public void newSession(@Nullable Transaction tx) {
        this.tx = tx;
        props = null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Transaction transaction() {
        return tx;
    }

    /** {@inheritDoc} */
    @Override public boolean isWithinTransaction() {
        return transaction() != null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object attach(@Nullable Object attach) {
        Object prev = this.attach;
        this.attach = attach;
        return prev;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T attachment() {
        return (T) attach;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> Map<K, V> properties() {
        return (Map<K, V>)props;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        return cacheName;
    }
}
