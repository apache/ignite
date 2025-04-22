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

package org.apache.ignite.internal.processors.platform.compute;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;

import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.readMap;

/** {@link ComputeTaskSession} platform wrapper. */
public class PlatformComputeTaskSession extends PlatformAbstractTarget {
    /** "get attribute" operation code. */
    private static final int OP_GET_ATTRIBUTE = 1;

    /** "set attributes" operation code. */
    private static final int OP_SET_ATTRIBUTES = 2;

    /** Underlying compute task session. */
    private final ComputeTaskSession ses;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param ses         Underlying compute task session
     */
    public PlatformComputeTaskSession(final PlatformContext platformCtx, final ComputeTaskSession ses) {
        super(platformCtx);

        this.ses = ses;
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(
        final int type, final BinaryReaderEx reader, final PlatformMemory mem) throws IgniteCheckedException {

        if (type == OP_SET_ATTRIBUTES) {
            final Map<?, ?> attrs = readMap(reader);

            ses.setAttributes(attrs);

            return TRUE;
        }

        return super.processInStreamOutLong(type, reader, mem);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(
        final int type, final BinaryReaderEx reader, final BinaryRawWriterEx writer) throws IgniteCheckedException {

        if (type == OP_GET_ATTRIBUTE) {
            final Object key = reader.readObjectDetached();

            final Object val = ses.getAttribute(key);

            writer.writeObjectDetached(val);

            return;
        }

        super.processInStreamOutStream(type, reader, writer);
    }
}
