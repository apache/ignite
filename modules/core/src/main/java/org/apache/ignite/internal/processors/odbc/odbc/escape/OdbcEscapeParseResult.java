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

package org.apache.ignite.internal.processors.odbc.odbc.escape;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC escape sequence parse result.
 */
public class OdbcEscapeParseResult {
    /** Original start position. */
    private final int originalStart;

    /** Original length. */
    private final int originalLen;

    /** Resulting text. */
    private final String res;

    /**
     * Constructor.
     *
     * @param originalStart Original start position.
     * @param originalLen Original length.
     * @param res Resulting text.
     */
    public OdbcEscapeParseResult(int originalStart, int originalLen, String res) {
        this.originalStart = originalStart;
        this.originalLen = originalLen;
        this.res = res;
    }

    /**
     * @return Original start position.
     */
    public int originalStart() {
        return originalStart;
    }

    /**
     * @return Original length.
     */
    public int originalLength() {
        return originalLen;
    }

    /**
     * @return Resulting text.
     */
    public String result() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcEscapeParseResult.class, this);
    }
}
