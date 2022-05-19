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

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.consistency.inmem.AtomicReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.ExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.ImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.ReplicatedExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.ReplicatedImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.SingleBackupExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.inmem.SingleBackupImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.persistence.PdsAtomicReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.persistence.PdsExplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.persistence.PdsImplicitTransactionalReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistentcut.ConcurrentTxsConsistentCutTest;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutBlockingNoBackupsTest;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutSingleBackupTest;
import org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutTwoBackupTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache consistency checks.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Inmem
    AtomicReadRepairTest.class,
    ExplicitTransactionalReadRepairTest.class,
    ImplicitTransactionalReadRepairTest.class,

    // PDS
    PdsAtomicReadRepairTest.class,
    PdsExplicitTransactionalReadRepairTest.class,
    PdsImplicitTransactionalReadRepairTest.class,

    // Special (inmem)
    ReplicatedExplicitTransactionalReadRepairTest.class,
    ReplicatedImplicitTransactionalReadRepairTest.class,
    SingleBackupExplicitTransactionalReadRepairTest.class,
    SingleBackupImplicitTransactionalReadRepairTest.class,

    // Consistent Cut
    ConsistentCutBlockingNoBackupsTest.class,
    ConsistentCutSingleBackupTest.class,
    ConsistentCutTwoBackupTest.class,
    ConcurrentTxsConsistentCutTest.class
})
public class IgniteCacheConsistencySelfTestSuite {
}
