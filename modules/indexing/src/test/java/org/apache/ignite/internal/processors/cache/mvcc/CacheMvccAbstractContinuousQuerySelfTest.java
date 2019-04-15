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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.query.continuous.GridCacheContinuousQueryAbstractSelfTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

/**
 *
 */
@RunWith(JUnit4.class)
public abstract class CacheMvccAbstractContinuousQuerySelfTest extends GridCacheContinuousQueryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL_SNAPSHOT;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testInternalKey() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testExpired() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7311");
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLoadCache() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7954");
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testEvents() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9321");
    }
}
