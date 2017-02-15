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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteEx;

/**
 *
 */
public class IgniteDbMemoryLeakLargePagesTest extends IgniteDbMemoryLeakTest {

    /** {@inheritDoc} */
    @Override protected void configure(MemoryConfiguration mCfg) {
        int concLvl = Runtime.getRuntime().availableProcessors();
        mCfg.setConcurrencyLevel(concLvl);
        mCfg.setPageCacheSize(1024 * 1024 * concLvl * 16);

    }

    /** {@inheritDoc} */
    @Override protected boolean isLargePage() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void check(IgniteEx ig) {
        long pages = ig.context().cache().context().database().pageMemory().loadedPages();

        assertTrue(pages < 4000);
    }
}
