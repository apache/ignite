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

import java.util.ArrayList;
import java.util.Arrays;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.LocalResult;
import org.h2.result.SortOrder;
import org.h2.util.Utils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;

/**
 * H2 local result.
 */
public class IgniteH2MemCheckLocalResult extends IgniteH2BaseLocalResult {
    /** Query context. */
    private final IgniteH2QueryMemoryManager mem;

    /** Allocated memory. */
    private long allocMem;

    /**
     * Construct a local result object.
     * @param mem Query memory manager.
     */
    public IgniteH2MemCheckLocalResult(IgniteH2QueryMemoryManager mem) {
        this.mem = mem;
    }

    /**
     * Construct a local result object.
     *
     * @param ses the session
     * @param expressions the expression array
     * @param visibleColCnt the number of visible columns
     * @param mem Query memory manager.
     */
    public IgniteH2MemCheckLocalResult(Session ses, Expression[] expressions, int visibleColCnt, IgniteH2QueryMemoryManager mem) {
        super(ses, expressions, visibleColCnt);

        assert mem != null;

        this.mem = mem;
    }

    /** {@inheritDoc} */
    @Override protected void checkAvailableMemory(Value... row) {
        for (Value v : row) {
            int size = v.getMemory();

            allocMem += size;

            mem.allocate(size);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        super.close();

        mem.free(allocMem);
    }
}
