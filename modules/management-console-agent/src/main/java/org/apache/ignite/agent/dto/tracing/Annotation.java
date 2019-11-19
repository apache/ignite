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

package org.apache.ignite.agent.dto.tracing;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for span annotation.
 */
public class Annotation {
    /** Timestamp. */
    private long ts;

    /** Value. */
    private String val;

    /**
     * Default constructor.
     */
    public Annotation() {
        // No-op.
    }

    /**
     * @param ts Timestamp.
     * @param val Value.
     */
    public Annotation(long ts, String val) {
        this.ts = ts;
        this.val = val;
    }

    /**
     * @return Timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @param ts Timestamp.
     * @return {@code This} for chaining method calls.
     */
    public Annotation setTimestamp(long ts) {
        this.ts = ts;

        return this;
    }

    /**
     * @return Value.
     */
    public String getValue() {
        return val;
    }

    /**
     * @param val Value.
     * @return {@code This} for chaining method calls.
     */
    public Annotation setValue(String val) {
        this.val = val;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Annotation.class, this);
    }
}
