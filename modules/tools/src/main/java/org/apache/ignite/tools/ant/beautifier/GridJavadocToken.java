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

package org.apache.ignite.tools.ant.beautifier;

/**
 * Lexical token.
 */
class GridJavadocToken {
    /** Token type. */
    private final GridJavadocTokenType type;

    /** Token value. */
    private String val;

    /**
     * Creates token.
     *
     * @param type Token type.
     * @param val Token value.
     */
    GridJavadocToken(GridJavadocTokenType type, String val) {
        assert type != null;
        assert val != null;

        this.type = type;
        this.val = val;
    }

    /**
     * Gets token type.
     *
     * @return Token type.
     */
    GridJavadocTokenType type() {
        return type;
    }

    /**
     * Gets token value.
     *
     * @return Token value.
     */
    String value() {
        return val;
    }

    /**
     * Sets new token value.
     *
     * @param val New token value.
     */
    void update(String val) {
        this.val = val;
    }
}