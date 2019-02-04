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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteTxExceptionAbstractSelfTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests replicated cache.
 */
public class GridCacheReplicatedTxExceptionSelfTest extends IgniteTxExceptionAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return REPLICATED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutAll() throws Exception {
        super.testPutAll();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutBackup() throws Exception {
        super.testPutBackup();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutBackupTx() throws Exception {
        super.testPutBackupTx();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutMultipleKeysTx() throws Exception {
        super.testPutMultipleKeysTx();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutNear() throws Exception {
        super.testPutNear();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutNearTx() throws Exception {
        super.testPutNearTx();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutPrimary() throws Exception {
        super.testPutPrimary();
    }

    /** */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-10377")
    @Test
    @Override public void testPutPrimaryTx() throws Exception {
        super.testPutPrimaryTx();
    }
}
