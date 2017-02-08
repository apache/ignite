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

package org.apache.ignite.platform.plugin;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.jetbrains.annotations.Nullable;

/**
 * Test target.
 */
class PlatformTestPluginTarget extends PlatformAbstractTarget {
    /** */
    private final String name;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     */
    PlatformTestPluginTarget(PlatformContext platformCtx, String name) {
        super(platformCtx);

        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        return val + 1;
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return reader.readString().length();
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        String s = reader.readString();

        writer.writeString(s.toUpperCase());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
            throws IgniteCheckedException {
        return new PlatformTestPluginTarget(platformCtx, reader.readString());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(
            int type, @Nullable PlatformTarget arg, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        PlatformTestPluginTarget t = (PlatformTestPluginTarget)arg;

        writer.writeString(t.name);

        return new PlatformTestPluginTarget(platformCtx, t.name + reader.readString());
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        writer.writeString(name);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        return new PlatformTestPluginTarget(platformCtx, name);
    }
}
