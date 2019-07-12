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

package org.apache.ignite.examples;

import org.apache.ignite.examples.misc.client.memcache.MemcacheRestExample;
import org.apache.ignite.examples.misc.client.memcache.MemcacheRestExampleNodeStartup;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 * MemcacheRestExample self test.
 */
public class MemcacheRestExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Override protected void beforeTest() throws Exception {
        // Start up a cluster node.
        startGrid("memcache-rest-examples", MemcacheRestExampleNodeStartup.configuration());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMemcacheRestExample() throws Exception {
        MemcacheRestExample.main(EMPTY_ARGS);
    }
}
