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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.sql.SqlKeyword;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;

import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseBooleanParameter;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;

/**
 * SET STREAMING command.
 */
public class SqlSetStreamingCommand implements SqlCommand {
    /** Default batch size for driver. */
    private final static int DFLT_STREAM_BATCH_SIZE = IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE * 4;

    /** Whether streaming must be turned on or off by this command. */
    private boolean turnOn;

    /** Whether existing values should be overwritten on keys duplication. */
    private boolean allowOverwrite;

    /** Batch size for driver. */
    private int batchSize = DFLT_STREAM_BATCH_SIZE;

    /** Per node number of parallel operations. */
    private int parOps;

    /** Per node buffer size. */
    private int bufSize;

    /** Streamer flush timeout. */
    private long flushFreq;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        turnOn = parseBooleanParameter(lex);

        while (lex.lookAhead().tokenType() == SqlLexerTokenType.DEFAULT) {
            switch (lex.lookAhead().token()) {
                case SqlKeyword.ALLOW_OVERWRITE:
                    lex.shift();

                    allowOverwrite = parseBooleanParameter(lex);

                    break;

                case SqlKeyword.PER_NODE_PARALLEL_OPERATIONS:
                    lex.shift();

                    parOps = parseInt(lex);

                    if (parOps <= 0)
                        throw error(lex, "Invalid per node parallel operations number - must be positive.");

                    break;

                case SqlKeyword.BATCH_SIZE:
                    lex.shift();

                    batchSize = parseInt(lex);

                    if (batchSize <= 0)
                        throw error(lex, "Invalid driver batch size - must be positive.");

                    break;

                case SqlKeyword.FLUSH_FREQUENCY:
                    lex.shift();

                    flushFreq = parseInt(lex);

                    if (flushFreq <= 0)
                        throw error(lex, "Invalid streamer flush frequency - must be positive.");

                    break;

                case SqlKeyword.PER_NODE_BUFFER_SIZE:
                    lex.shift();

                    bufSize = parseInt(lex);

                    if (bufSize <= 0)
                        throw error(lex, "Invalid per node buffer size - must be positive.");

                    break;

                default:
                    return this;
            }
        }

        return this;
    }

    /**
     * @return Whether streaming must be turned on or off by this command.
     */
    public boolean isTurnOn() {
        return turnOn;
    }

    /**
     * @return Whether existing values should be overwritten on keys duplication.
     */
    public boolean isAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * @return Batch size for driver.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * @return Per node number of parallel operations.
     */
    public int perNodeParallelOperations() {
        return parOps;
    }

    /**
     * @return Per node streamer buffer size.
     */
    public int perNodeBufferSize() {
        return bufSize;
    }

    /**
     * @return Streamer flush timeout
     */
    public long flushFrequency() {
        return flushFreq;
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
