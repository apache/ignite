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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

/**
 *
 */
public class IgniteWalFlushLogOnlyWithMmapBufferSelfTest extends IgniteWalFlushLogOnlySelfTest {
    /** {@inheritDoc} */
    @Override protected boolean mmap() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void testFailWhileStart() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8727");

        super.testFailWhileStart();
    }

    /** {@inheritDoc} */
    @Override public void testFailAfterStart() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-8727");

        super.testFailAfterStart();
    }
}
