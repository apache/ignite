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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryThreadLocalContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryHeapOutputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerAbstractMessageParser;

/**
 * JDBC message parser.
 */
public class OdbcMessageParser extends SqlListenerAbstractMessageParser {
    /** Marshaller. */
    private final GridBinaryMarshaller marsh;

    /**
     * @param ctx Context.
     */
    public OdbcMessageParser(GridKernalContext ctx) {
        super(ctx, new OdbcObjectReader(), new OdbcObjectWriter());

        if (ctx.cacheObjects() instanceof CacheObjectBinaryProcessorImpl) {
            CacheObjectBinaryProcessorImpl cacheObjProc = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

            marsh = cacheObjProc.marshaller();
        }
        else {
            throw new IgniteException("ODBC can only be used with BinaryMarshaller (please set it " +
                "through IgniteConfiguration.setMarshaller())");
        }
    }

    /** {@inheritDoc} */
    @Override protected BinaryReaderExImpl createReader(byte[] msg) {
        BinaryInputStream stream = new BinaryHeapInputStream(msg);

        return new BinaryReaderExImpl(marsh.context(), stream, ctx.config().getClassLoader(), true);
    }

    /** {@inheritDoc} */
    @Override protected BinaryWriterExImpl createWriter(int cap) {
        return new BinaryWriterExImpl(marsh.context(), new BinaryHeapOutputStream(cap),
            BinaryThreadLocalContext.get().schemaHolder(), null);
    }
}
