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

package org.apache.ignite.cache.store.jdbc;

import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Test for JDBC POJO store from multiple threads.
 */
public class CacheJdbcPojoStoreMultitreadedSelfTest
    extends CacheJdbcStoreAbstractMultithreadedSelfTest<CacheJdbcPojoStore> {
    /** {@inheritDoc} */
    @Override protected CacheJdbcPojoStore store() throws Exception {
        CacheJdbcPojoStore store = new CacheJdbcPojoStore();

        store.setDataSource(JdbcConnectionPool.create(DFLT_CONN_URL, "sa", ""));

        return store;
    }

    /** {@inheritDoc} */
    @Override public void testMultithreadedPut() throws Exception {
        if (!supportedMarshaller())
            fail("https://ggsystems.atlassian.net/browse/GG-13127");

        super.testMultithreadedPut();
    }

    /** {@inheritDoc} */
    @Override public void testMultithreadedPutAll() throws Exception {
        if (!supportedMarshaller())
            fail("https://ggsystems.atlassian.net/browse/GG-13127");

        super.testMultithreadedPutAll();
    }

    /** {@inheritDoc} */
    @Override public void testMultithreadedExplicitTx() throws Exception {
        if (!supportedMarshaller())
            fail("https://ggsystems.atlassian.net/browse/GG-13127");

        super.testMultithreadedExplicitTx();
    }

    /**
     * @return Supported marshaller.
     */
    private boolean supportedMarshaller() {
        Marshaller marsh = grid().configuration().getMarshaller();

        return marsh instanceof BinaryMarshaller || marsh instanceof OptimizedMarshaller;
    }
}