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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Base class for all GGFS communication messages sent between nodes.
 */
public abstract class GridGgfsCommunicationMessage extends MessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
    }

    /**
     * @param marsh Marshaller.
     * @throws IgniteCheckedException In case of error.
     */
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws IgniteCheckedException In case of error.
     */
    public void finishUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        writer.setBuffer(buf);

        if (!typeWritten) {
            if (!writer.writeByte(null, directType()))
                return false;

            typeWritten = true;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        return true;
    }
}
