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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.util.typedef.F;

/**
 * Parser utility methods.
 */
public class SqlParserUtils {
    /**
     * Create parse exception referring to current lexer position.
     *
     * @param lex Lexer.
     * @param msg Message.
     * @return Exception.
     */
    public static SqlParseException error(SqlLexer lex, String msg) {
        return new SqlParseException(lex.input(), lex.tokenPosition(), msg);
    }

    /**
     * Create generic parse exception due to unexpected token.
     *
     * @param lex Lexer.
     * @param expTokens Expected tokens (if any).
     * @return Exception.
     */
    public static SqlParseException errorUnexpectedToken(SqlLexer lex, String... expTokens) {
        String token = lex.token();

        StringBuilder msg = new StringBuilder(
            token == null ? "Unexpected end of command" : "Unexpected token: " + token);

        if (!F.isEmpty(expTokens)) {
            msg.append(" (expected: ");

            boolean first = true;

            for (String expToken : expTokens) {
                if (first)
                    first = false;
                else
                    msg.append(", ");

                msg.append(expToken);
            }

            msg.append(")");
        }

        throw error(lex, msg.toString());
    }

    /**
     * Private constructor.
     */
    private SqlParserUtils() {
        // No-op.
    }
}
