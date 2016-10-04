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

package org.apache.ignite.internal.processors.platform.plugin;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.PlatformPluginContext;
import org.apache.ignite.plugin.PlatformPluginTarget;
import org.jetbrains.annotations.Nullable;

/**
 * Adapts user-defined IgnitePlatformPluginTarget to internal PlatformAbstractTarget.
 */
public class PlatformPluginTargetAdapter extends PlatformAbstractTarget implements PlatformPluginContext {
    /** User-defined target. */
    private final PlatformPluginTarget target;

    /** Plugin name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param target User-defined target.
     * @param name Plugin name.
     */
    public PlatformPluginTargetAdapter(PlatformContext platformCtx, PlatformPluginTarget target, String name) {
        super(platformCtx);

        assert target != null;
        assert name != null;

        this.target = target;
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override protected Object processInObjectStreamOutObjectStream(int type, @Nullable Object arg,
        BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {
        PlatformPluginTarget arg0 = arg == null ? null : ((PlatformPluginTargetAdapter)arg).target;

        PlatformPluginTarget res = target.invokeOperation(type, reader, writer, arg0, this);

        return res == null ? null : new PlatformPluginTargetAdapter(platformCtx, res, name);
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        return platformCtx.kernalContext().grid();
    }

    /** {@inheritDoc} */
    @Override public <T> T callback(IgniteInClosure<BinaryRawWriter> writeClosure,
        IgniteClosure<BinaryRawReader, T> readClosure) {

        try (PlatformMemory outMem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = outMem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeString(name);

            if (writeClosure != null)
                writeClosure.apply(writer);

            out.synchronize();

            PlatformMemory inMem = null;
            long inMemPtr = 0;

            if (readClosure != null) {
                inMem = platformCtx.memory().allocate();
                inMemPtr = inMem.pointer();
            }

            platformCtx.gateway().extensionCallbackInLongLongOutLong(
                PlatformUtils.OP_PLUGIN_CALLBACK, outMem.pointer(), inMemPtr);

            if (readClosure == null)
                return null;

            return readClosure.apply(platformCtx.reader(inMem.input()));
        }
    }
}
