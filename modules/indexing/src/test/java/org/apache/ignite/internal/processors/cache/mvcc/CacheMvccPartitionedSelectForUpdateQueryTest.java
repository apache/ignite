/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.cache.CacheMode;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/** */
public class CacheMvccPartitionedSelectForUpdateQueryTest extends CacheMvccSelectForUpdateQueryAbstractTest {
    /** {@inheritDoc} */
    public CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     *
     */
    public void testSelectForUpdateDistributedSegmented() throws Exception {
        doTestSelectForUpdateDistributed("PersonSeg", false);
    }

    /**
     *
     */
    public void testSelectForUpdateLocalSegmented() throws Exception {
        doTestSelectForUpdateLocal("PersonSeg", false);
    }
}
