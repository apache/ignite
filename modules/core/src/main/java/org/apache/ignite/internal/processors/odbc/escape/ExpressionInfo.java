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

package org.apache.ignite.internal.processors.odbc.escape;

/**
 * ODBC escape sequence expression parsed info.
 */
public class ExpressionInfo {
    /** Expression start position */
    private final int expressionStart;

    /** Expression length */
    private final int expressionLen;

    /** Escape type */
    private final OdbcEscapeType type;

    /**
     * Constructor.
     *
     * @param expressionStart Expression start position.
     * @param expressionLen Expression length.
     * @param type Escape type
     */
    public ExpressionInfo(int expressionStart, int expressionLen,
        OdbcEscapeType type) {
        this.expressionStart = expressionStart;
        this.expressionLen = expressionLen;
        this.type = type;
    }

    /**
     * @return Expression start position
     */
    public int expressionStart() {
        return expressionStart;
    }

    /**
     * @return Expression length
     */
    public int expressionLen() {
        return expressionLen;
    }

    /**
     * @return Escape type
     */
    public OdbcEscapeType type() {
        return type;
    }
}
