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

package org.apache.ignite.internal.processors.platform.datastreamer;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractPredicate;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxy;
import org.apache.ignite.internal.processors.platform.PlatformTargetProxyImpl;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;

/**
 * Interop receiver.
 */
public class PlatformStreamReceiverImpl extends PlatformAbstractPredicate implements PlatformStreamReceiver {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean keepBinary;

    /**
     * Constructor.
     */
    public PlatformStreamReceiverImpl()
    {
        super();
    }

    /**
     * Constructor.
     *
     * @param pred .Net binary receiver.
     * @param ptr Pointer to receiver in the native platform.
     * @param ctx Kernal context.
     */
    public PlatformStreamReceiverImpl(Object pred, long ptr, boolean keepBinary, PlatformContext ctx) {
        super(pred, ptr, ctx);

        assert pred != null;

        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<Object, Object> cache, Collection<Map.Entry<Object, Object>> collection)
        throws IgniteException {
        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            out.writeLong(ptr);
            out.writeBoolean(keepBinary);

            BinaryRawWriterEx writer = ctx.writer(out);

            writer.writeObject(pred);

            writer.writeInt(collection.size());

            for (Map.Entry<Object, Object> e : collection) {
                writer.writeObject(e.getKey());
                writer.writeObject(e.getValue());
            }

            out.synchronize();

            PlatformCache cache0 = new PlatformCache(ctx, cache, keepBinary);
            PlatformTargetProxy cacheProxy = new PlatformTargetProxyImpl(cache0, ctx);

            ctx.gateway().dataStreamerStreamReceiverInvoke(ptr, cacheProxy, mem.pointer(), keepBinary);
        }
    }

    /**
     * @param ignite Ignite instance.
     */
    @SuppressWarnings("UnusedDeclaration")
    @IgniteInstanceResource
    public void setIgniteInstance(Ignite ignite) {
        ctx = PlatformUtils.platformContext(ignite);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(keepBinary);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        keepBinary = in.readBoolean();
    }
}
