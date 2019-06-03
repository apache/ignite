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

package org.apache.ignite.internal.processors.query.h2;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.H2BaseLocalResult;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultFactory;

/**
 * Ignite implementation of the H2 local result factory.
 */
public class H2LocalResultFactory extends LocalResultFactory {
    /** {@inheritDoc} */
    @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt) {
        H2MemoryTracker memoryTracker = ses.queryMemoryTracker();

        if (memoryTracker != null)
            return new H2ManagedLocalResult(ses, memoryTracker, expressions, visibleColCnt);

        return new H2BaseLocalResult(ses, expressions, visibleColCnt);
    }

    /** {@inheritDoc} */
    @Override public LocalResult create() {
        return new H2BaseLocalResult();
    }
}
