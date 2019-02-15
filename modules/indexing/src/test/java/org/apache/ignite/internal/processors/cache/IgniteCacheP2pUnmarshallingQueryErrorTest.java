/*
 *                    GridGain Community Edition Licensing
 *                    Copyright 2019 GridGain Systems, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 *  Restriction; you may not use this file except in compliance with the License. You may obtain a
 *  copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 *  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the specific language governing permissions
 *  and limitations under the License.
 *
 *  Commons Clause Restriction
 *
 *  The Software is provided to you by the Licensor under the License, as defined below, subject to
 *  the following condition.
 *
 *  Without limiting other conditions in the License, the grant of rights under the License will not
 *  include, and the License does not grant to you, the right to Sell the Software.
 *  For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 *  under the License to provide to third parties, for a fee or other consideration (including without
 *  limitation fees for hosting or consulting/ support services related to the Software), a product or
 *  service whose value derives, entirely or substantially, from the functionality of the Software.
 *  Any license notice or attribution required by the License must also include this Commons Clause
 *  License Condition notice.
 *
 *  For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 *  the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 *  Edition software provided with this notice.
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checks behavior on exception while unmarshalling key.
 */
@RunWith(JUnit4.class)
public class IgniteCacheP2pUnmarshallingQueryErrorTest extends IgniteCacheP2pUnmarshallingErrorTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.getCacheConfiguration().length > 0)
            cfg.getCacheConfiguration()[0].setIndexedTypes(TestKey.class, String.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Test
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
    @Test
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
