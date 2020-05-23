/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.tracing;

/**
 * Tracing span scope.
 */
public enum Scope {
    /** Discovery scope. */
    DISCOVERY((short)1),

    /** Exchange scope. */
    EXCHANGE((short)2),

    /** Communication scope. */
    COMMUNICATION((short)3),

    /** Transactional scope. */
    TX((short)4);

    /** Scope index. */
    private final short idx;

    /** Values. */
    private static final Scope[] VALS;

    /**
     * Constructor.
     *
     * @param idx Scope index.
     */
    Scope(short idx) {
        this.idx = idx;
    }

    /**
     * @return Id.
     */
    public short idx() {
        return idx;
    }

    static {
        Scope[] scopes = Scope.values();

        int maxIdx = 0;

        for (Scope scope : scopes)
            maxIdx = Math.max(maxIdx, scope.idx);

        VALS = new Scope[maxIdx + 1];

        for (Scope scope : scopes)
            VALS[scope.idx] = scope;
    }

    /**
     * Created Scope from it's index.
     * @param idx Index.
     * @return Scope.
     */
    public static Scope fromIndex(short idx) {
        return idx < 0 || idx >= VALS.length ? null : VALS[idx];
    }
}
