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
package org.apache.ignite.internal.processors.cache.persistence;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Version of test to be executed in Direct IO suite.
 * Contains reduced number of records, because Direct IO does not support tmpfs.
 */
@RunWith(JUnit4.class)
public class IgniteNativeIoLocalWalModeChangeDuringRebalancingSelfTest extends LocalWalModeChangeDuringRebalancingSelfTest {
    /** {@inheritDoc} */
    @Override protected int getKeysCount() {
        return 1_000;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testWithExchangesMerge() throws Exception {


        super.testWithExchangesMerge();
    }
}
