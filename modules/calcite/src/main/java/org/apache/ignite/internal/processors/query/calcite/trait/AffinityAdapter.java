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

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.function.ToIntFunction;
import org.apache.ignite.internal.processors.query.calcite.exec.RowHandler;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 *
 */
final class AffinityAdapter<RowT> implements ToIntFunction<RowT> {
    /**
     *
     */
    private final ToIntFunction<Object> affinity;

    /**
     *
     */
    private final int[] keys;

    /**
     *
     */
    private final RowHandler<RowT> hndlr;

    /**
     *
     */
    AffinityAdapter(ToIntFunction<Object> affinity, int[] keys, RowHandler<RowT> hndlr) {
        this.affinity = affinity;
        this.keys = keys;
        this.hndlr = hndlr;
    }

    /** {@inheritDoc} */
    @Override
    public int applyAsInt(RowT r) {
        int hash = 0;
        for (int i = 0; i < keys.length; i++) {
            hash = 31 * hash + affinity.applyAsInt(hndlr.get(keys[i], r));
        }

        return IgniteUtils.safeAbs(hash);
    }
}
