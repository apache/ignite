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

package org.apache.ignite.internal.util.typedef.internal;

import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Defines internal {@code typedef} for {@link GridStringBuilder}. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended for internal
 * use only and meant to provide for more terse code when readability of code is not compromised.
 */
public class SB extends GridStringBuilder {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @see GridStringBuilder#GridStringBuilder()
     */
    public SB() {
        super(16);
    }

    /**
     *
     * @param cap Capacity.
     * @see GridStringBuilder#GridStringBuilder(int)
     */
    public SB(int cap) {
        super(cap);
    }

    /**
     *
     * @param str String.
     * @see GridStringBuilder#GridStringBuilder(String)
     */
    public SB(String str) {
        super(str);
    }

    /**
     * @param seq Sequence.
     * @see GridStringBuilder#GridStringBuilder(CharSequence)
     */
    public SB(CharSequence seq) {
        super(seq);
    }
}