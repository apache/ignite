/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import javax.cache.CacheException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Checks behavior on exception while unmarshalling key.
 */
public class IgniteCacheP2pUnmarshallingQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cfg.getCacheConfiguration().length > 0)
            cfg.getCacheConfiguration()[0].setIndexedTypes(TestKey.class, String.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testResponseMessageOnUnmarshallingFailed() {
        readCnt.set(Integer.MAX_VALUE);

        jcache(0).put(new TestKey(String.valueOf(++key)), "");

        //GridCacheQueryRequest unmarshalling failed test
        readCnt.set(1);

        try {
            jcache(0).query(new SqlQuery<TestKey, String>(String.class, "field like '" + key + "'")).getAll();

            fail("p2p marshalling failed, but error response was not sent");
        }
        catch (CacheException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testResponseMessageOnRequestUnmarshallingFailed() throws Exception {
        readCnt.set(Integer.MAX_VALUE);

        try {
            jcache().query(new ScanQuery<>(new IgniteBiPredicate<TestKey, String>() {
                @Override public boolean apply(TestKey key, String val) {
                    return false;
                }

                private void readObject(ObjectInputStream is) throws IOException {
                    throw new IOException();
                }

                private void writeObject(ObjectOutputStream os) throws IOException {
                    // No-op.
                }
            })).getAll();

            fail();
        }
        catch (Exception ignored) {
            // No-op.
        }
    }
}
