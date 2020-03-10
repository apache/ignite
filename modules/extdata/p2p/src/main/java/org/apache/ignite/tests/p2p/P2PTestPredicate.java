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
package org.apache.ignite.tests.p2p;

import java.io.Serializable;
import org.apache.ignite.internal.processors.cache.GridCacheDeployable;
import org.apache.ignite.internal.processors.cache.GridCacheIdMessage;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Test predicate for scan queries in p2p deployment tests.
 */
@SuppressWarnings("unused")
public class P2PTestPredicate extends GridCacheIdMessage implements GridCacheDeployable, IgniteBiPredicate, Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Object o, Object o2) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 0;
    }
}
