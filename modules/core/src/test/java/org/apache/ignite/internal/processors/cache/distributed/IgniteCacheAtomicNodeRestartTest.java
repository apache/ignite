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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;

/**
 *
 */
public class IgniteCacheAtomicNodeRestartTest extends GridCachePartitionedNodeRestartTest {
    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }

    public void testRestartWithPutTenNodesTwoBackups2() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups3() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups4() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups5() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups6() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups7() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups8() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups9() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups10() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups11() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups12() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups13() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }
    public void testRestartWithPutTenNodesTwoBackups14() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }

    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    @Override public void testRestart() throws Exception {

    }

    @Override public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
    }

    @Override public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
    }

    @Override public void testRestartWithPutFourNodesNoBackups() throws Throwable {
    }

    @Override public void testRestartWithPutFourNodesOneBackups() throws Throwable {
    }

    @Override public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
    }

    @Override public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxFourNodesNoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
    }

    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
    }

    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
    }
}
