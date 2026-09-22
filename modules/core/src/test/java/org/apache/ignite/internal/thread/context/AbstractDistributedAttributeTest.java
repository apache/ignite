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

package org.apache.ignite.internal.thread.context;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.thread.context.DistributedAttributeKeyRegistry.VALS;

/** */
public abstract class AbstractDistributedAttributeTest extends GridCommonAbstractTest {
    /** */
    private DistributedAttributeKey[] originalAttrKeys;

    /** */
    protected Collection<DistributedAttributeKey> distributedAttributeKeys() {
        return Collections.emptyList();
    }

    /** */
    protected static DistributedAttributeKey createTestKey(int id) {
        return new DistributedAttributeKey(id);
    }

    /** */
    protected static DistributedAttributeKey createTestKey(int id, IgniteFeature introducedBy) {
        return new DistributedAttributeKey(id, introducedBy);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Collection<DistributedAttributeKey> testKeys = distributedAttributeKeys();

        if (!testKeys.isEmpty()) {
            originalAttrKeys = Arrays.copyOf(VALS, VALS.length);

            for (DistributedAttributeKey key : testKeys) {
                VALS[key.id()] = key;
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        if (originalAttrKeys != null) {
            System.arraycopy(originalAttrKeys, 0, VALS, 0, originalAttrKeys.length);

            originalAttrKeys = null;
        }

        super.afterTestsStopped();
    }
}
