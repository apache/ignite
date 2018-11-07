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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.result.LocalResult;
import org.h2.result.LocalResultFactory;

/**
 * Ignite implementation of the H2 local result factory.
 */
public class IgniteH2LocalResultFactory extends LocalResultFactory {
    /** {@inheritDoc} */
    @Override public LocalResult create(Session ses, Expression[] expressions, int visibleColCnt) {
        IgniteH2QueryMemoryManager memMgr = memoryManager();

        if (memMgr != null)
            return new IgniteH2MemCheckLocalResult(ses, expressions, visibleColCnt, memMgr);
        else
            return new IgniteH2BaseLocalResult(ses, expressions, visibleColCnt);
    }

    /** {@inheritDoc} */
    @Override public LocalResult create() {
        IgniteH2QueryMemoryManager memMgr = memoryManager();

        if (memMgr != null)
            return new IgniteH2MemCheckLocalResult(memMgr);
        else
            return new IgniteH2BaseLocalResult();
    }

    /**
     * @return Gathers memory manager fro query context.
     */
    private IgniteH2QueryMemoryManager memoryManager() {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        return (qctx != null) ?  qctx.queryMemoryManager() : null;
    }
}
