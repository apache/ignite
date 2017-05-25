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

package org.apache.ignite.internal.processors.odbc.jdbc;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.odbc.SqlListenerAbstractMessageParser;

/**
 * JDBC message parser.
 */
public class JdbcMessageParser extends SqlListenerAbstractMessageParser {
    /**
     * @param ctx Context.
     */
    public JdbcMessageParser(GridKernalContext ctx) {
        super(ctx, new JdbcObjectReader(), new JdbcObjectWriter());
    }

    /** {@inheritDoc} */
    @Override protected BinaryReaderExImpl createReader(byte[] msg) {
        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        return new BinaryReaderExImpl(null, stream, ctx.config().getClassLoader(), true);
    }

    /** {@inheritDoc} */
    @Override protected BinaryWriterExImpl createWriter(int cap) {
        return new BinaryWriterExImpl(null, new BinaryHeapOutputStream(cap), null, null);
    }
}
