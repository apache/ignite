/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.cache.LargeEntryUpdateTest;
import org.apache.ignite.internal.processors.database.IgniteDbMemoryLeakLargeObjectsTest;
import org.apache.ignite.internal.processors.database.IgniteDbMemoryLeakLargePagesTest;
import org.apache.ignite.internal.processors.database.IgniteDbMemoryLeakNonTransactionalTest;
import org.apache.ignite.internal.processors.database.IgniteDbMemoryLeakTest;
import org.apache.ignite.internal.processors.database.IgniteDbMemoryLeakWithExpirationTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Page memory leaks tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteDbMemoryLeakTest.class,
    IgniteDbMemoryLeakWithExpirationTest.class,
    IgniteDbMemoryLeakLargePagesTest.class,
    IgniteDbMemoryLeakLargeObjectsTest.class,
    IgniteDbMemoryLeakNonTransactionalTest.class,

    LargeEntryUpdateTest.class
})
public class IgniteDbMemoryLeakTestSuite {
}
