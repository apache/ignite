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

import org.apache.ignite.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.processors.platform.cache.*;
import org.apache.ignite.internal.processors.platform.memory.*;
import org.apache.ignite.internal.processors.platform.utils.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.stream.*;

import java.io.*;
import java.util.*;

/**
 * Interop receiver.
 */
public class PlatformStreamReceiver<K, V> extends PlatformAbstractPredicate implements StreamReceiver<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private boolean keepPortable;

    /**
     * Constructor.
     */
    public PlatformStreamReceiver()
    {
        super();
    }

    /**
     * Constructor.
     *
     * @param pred .Net portable receiver.
     * @param ptr Pointer to receiver in the native platform.
     * @param ctx Kernal context.
     */
    public PlatformStreamReceiver(Object pred, long ptr, boolean keepPortable, PlatformContext ctx) {
        super(pred, ptr, ctx);

        assert pred != null;

        this.keepPortable = keepPortable;
    }

    /** {@inheritDoc} */
    @Override public void receive(IgniteCache<K, V> cache, Collection<Map.Entry<K, V>> collection)
        throws IgniteException {
        assert ctx != null;

        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = ctx.writer(out);

            writer.writeObject(pred);

            writer.writeInt(collection.size());

            for (Map.Entry<K, V> e : collection) {
                writer.writeObject(e.getKey());
                writer.writeObject(e.getValue());
            }

            out.synchronize();

            ctx.gateway().dataStreamerStreamReceiverInvoke(ptr,
                new PlatformCache(ctx, cache, keepPortable), mem.pointer(), keepPortable);
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

        out.writeBoolean(keepPortable);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        keepPortable = in.readBoolean();
    }

}
