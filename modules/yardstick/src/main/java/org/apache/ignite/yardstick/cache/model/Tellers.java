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

package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Entity class for benchmark.
 */
public class Tellers {
    /** */
    @QuerySqlField
    private long val;

    /** */
    public Tellers() {
        // No-op.
    }

    /**
     * @param val Id.
     */
    public Tellers(long val) {
        this.val = val;
    }

    /**
     * @param val Val.
     */
    public Tellers setVal(long val) {
        this.val = val;

        return this;
    }

    /**
     * @return Val.
     */
    public long getVal() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Tellers [val=" + val + ']';
    }
}