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

package org.apache.ignite.testframework.junits.cache;

import java.util.Map;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class TestThreadLocalCacheSession implements CacheStoreSession {
    /** */
    private final ThreadLocal<TestCacheSession> sesHolder = new ThreadLocal<>();

    /**
     * @param tx Transaction.
     */
    public void newSession(@Nullable Transaction tx) {
        TestCacheSession ses = new TestCacheSession();

        ses.newSession(tx);

        sesHolder.set(ses);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Transaction transaction() {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? ses.transaction() : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isWithinTransaction() {
        return transaction() != null;
    }

    /** {@inheritDoc} */
    @Override public Object attach(@Nullable Object attachment) {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? ses.attach(attachment) : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T attachment() {
        TestCacheSession ses = sesHolder.get();

        return ses!= null ? (T)ses.attachment() : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> Map<K, V> properties() {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? (Map<K, V>)ses.properties() : null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String cacheName() {
        TestCacheSession ses = sesHolder.get();

        return ses != null ? ses.cacheName() : null;
    }
}