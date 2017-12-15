/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal.reader;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;

/** Standalone test to run verification on existent store */
public class IgniteMassLoadVerifySandboxTest extends IgniteMassLoadSandboxTest {

    @Override protected void beforeTestsStarted() throws Exception {
        // No-op. //super.beforeTestsStarted();
    }

    @Override protected void beforeTest() throws Exception {
        // no-op. super.beforeTest();
    }

    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return super.getConfiguration(gridName);
    }

    public void testGetAndVerifyMultithreaded() throws Exception {
        try {
            System.setProperty(IgniteSystemProperties.IGNITE_DIRECT_IO_ENABLED, "true");

            setWalArchAndWorkToSameValue = true;
            customWalMode = WALMode.LOG_ONLY;

            final int threads = 16;

            int recsPerThread = CONTINUOUS_PUT_RECS_CNT / threads;
            runVerification(threads, recsPerThread);
        }
        finally {
            stopAllGrids();
        }
    }

}
