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

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.internal.portable.BinaryMarshaller;

/**
 * Class for {@code PojoCacheStore} tests.
 */
public class CacheJdbcPojoStoreBinaryMarshallerSelfTest extends CacheJdbcPojoStoreAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Marshaller marshaller(){
        return new BinaryMarshaller();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyClasses() throws Exception {
        startTestGrid(false, true, false, false);

        checkCacheContent();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyClassesTx() throws Exception {
        startTestGrid(false, true, false, true);

        checkCacheContent();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoValueClasses() throws Exception {
        startTestGrid(false, false, true, false);

        checkCacheContent();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoValueClassesTx() throws Exception {
        startTestGrid(false, false, true, true);

        checkCacheContent();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyAndValueClasses() throws Exception {
        startTestGrid(false, true, true, false);

        checkCacheContent();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyAndValueClassesTx() throws Exception {
        startTestGrid(false, true, true, true);

        checkCacheContent();
    }
}
