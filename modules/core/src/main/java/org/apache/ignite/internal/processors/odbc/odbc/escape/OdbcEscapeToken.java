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

package org.apache.ignite.internal.processors.odbc.odbc.escape;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC escape sequence token.
 */
public class OdbcEscapeToken {
    /** Escape sequence type. */
    private final OdbcEscapeType type;

    /** Token length. */
    private final int len;

    /**
     * Constructor.
     *
     * @param type Escape sequence type.
     * @param len Token length.
     */
    public OdbcEscapeToken(OdbcEscapeType type, int len) {
        this.type = type;
        this.len = len;
    }

    /**
     * @return Escape sequence type.
     */
    public OdbcEscapeType type() {
        return type;
    }

    /**
     * @return Token length.
     */
    public int length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcEscapeToken.class, this);
    }
}
