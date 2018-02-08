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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;

/**
 * SET STREAMING command.
 */
public class SqlSetStreamingCommand implements SqlCommand {
    /** Whether streaming must be turned on or off by this command. */
    private boolean turnOn;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.token()) {
                case SqlKeyword.ON:
                case "1":
                    turnOn = true;

                    return this;

                case SqlKeyword.OFF:
                case "0":
                    turnOn = false;

                    return this;
            }
        }

        throw errorUnexpectedToken(lex, SqlKeyword.ON, SqlKeyword.OFF, "1", "0");
    }

    /**
     * @return Whether streaming must be turned on or off by this command.
     */
    public boolean isTurnOn() {
        return turnOn;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }
}
