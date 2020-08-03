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
package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.internal.processors.cache.IgniteCacheEntryListenerAtomicTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Class is added to mute {@link #testConcurrentRegisterDeregister} test in ZooKeeper suite
 * (see related ticket).
 *
 * When slow down is tracked down and fixed this class can be replaced back with its parent.
 */
public class IgniteCacheEntryListenerWithZkDiscoAtomicTest extends IgniteCacheEntryListenerAtomicTest {
    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8109")
    @Test
    @Override public void testConcurrentRegisterDeregister() throws Exception {
        // No-op.
    }
}
