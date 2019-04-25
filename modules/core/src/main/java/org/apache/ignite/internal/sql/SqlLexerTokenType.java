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

package org.apache.ignite.internal.sql;

import java.util.HashMap;

/**
 * Lexer token type.
 */
public enum SqlLexerTokenType {
    /** Standard word. */
    DEFAULT,

    /** Quoted phrase. */
    QUOTED,

    /** Quoted string constant. */
    STRING,

    /** Minus sign. */
    MINUS('-'),

    /** Dot. */
    DOT('.'),

    /** Comma. */
    COMMA(','),

    /** Parenthesis: left. */
    PARENTHESIS_LEFT('('),

    /** Parenthesis: right. */
    PARENTHESIS_RIGHT(')'),

    /** Semicolon. */
    SEMICOLON(';'),

    /** End of string. */
    EOF;

    /** Mapping from character to type.. */
    private static final HashMap<Character, SqlLexerTokenType> CHAR_TO_TYP = new HashMap<>();
    
    /** Character. */
    private final Character c;

    /** Character as string. */
    private final String str;

    static {
        for (SqlLexerTokenType typ : SqlLexerTokenType.values()) {
            Character c = typ.asChar();

            if (c != null)
                CHAR_TO_TYP.put(c, typ);
        }
    }

    /**
     * Get token type for character.
     *
     * @param c Character.
     * @return Type.
     */
    public static SqlLexerTokenType forChar(char c) {
        return CHAR_TO_TYP.get(c);
    }

    /**
     * Constructor.
     */
    SqlLexerTokenType() {
        this(null);
    }

    /**
     * Constructor.
     * 
     * @param c Corresponding character.
     */
    SqlLexerTokenType(Character c) {
        this.c = c;
        
        str = c != null ? c.toString() : null;
    }

    /**
     * @return Character.
     */
    public Character asChar() {
        return c;
    }
    
    /**
     * @return Character as string.
     */
    public String asString() {
        return str;
    }
}
