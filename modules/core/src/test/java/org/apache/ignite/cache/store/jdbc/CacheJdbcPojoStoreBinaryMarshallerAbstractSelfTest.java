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

import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract test for {@link CacheJdbcPojoStore} with binary marshaller.
 * Marshaller's string encoding should be specified in subclasses.
 */
public abstract class CacheJdbcPojoStoreBinaryMarshallerAbstractSelfTest extends CacheJdbcPojoStoreAbstractSelfTest {
    /**
     * Provides string encoding to be used by {@link BinaryMarshaller}.
     *
     * @return Encoding name; {@code null} refers to default (UTF-8) encoding.
     */
    protected abstract @Nullable String stringEncoding();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        if (bCfg == null) {
            bCfg = new BinaryConfiguration();

            cfg.setBinaryConfiguration(bCfg);
        }

        bCfg.setEncoding(stringEncoding());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected Marshaller marshaller(){
        return new BinaryMarshaller();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyClasses() throws Exception {
        startTestGrid(false, true, false, false, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyClassesTx() throws Exception {
        startTestGrid(false, true, false, true, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoValueClasses() throws Exception {
        startTestGrid(false, false, true, false, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoValueClassesTx() throws Exception {
        startTestGrid(false, false, true, true, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyAndValueClasses() throws Exception {
        startTestGrid(false, true, true, false, 512);

        checkCacheLoad();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoadCacheNoKeyAndValueClassesTx() throws Exception {
        startTestGrid(false, true, true, true, 512);

        checkCacheLoad();
    }
}
