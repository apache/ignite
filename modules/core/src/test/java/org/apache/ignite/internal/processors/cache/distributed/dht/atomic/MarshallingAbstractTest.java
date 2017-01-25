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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryCachingMetadataHandler;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryContext;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public abstract class MarshallingAbstractTest extends TestCase {

    /** */
    private static final byte proto = 2;

    /** */
    private GridCacheSharedContext ctx;

    /** */
    private GridCacheContext cctx;

    /** */
    @Override protected void setUp() throws Exception {
        super.setUp();
        ctx = mock(GridCacheSharedContext.class);
        cctx = mock(GridCacheContext.class);

        CacheObjectBinaryContext coctx = mock(CacheObjectBinaryContext.class);
        GridKernalContext kctx = mock(GridKernalContext.class);
        IgniteEx ignite = mock(IgniteEx.class);
        GridCacheProcessor proc = mock(GridCacheProcessor.class);

        Marshaller marsh = new BinaryMarshaller();

        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setMarshaller(marsh);

        when(ctx.cacheContext(anyInt())).thenReturn(cctx);
        when(ctx.marshaller()).thenReturn(marsh);
        when(ctx.gridConfig()).thenReturn(cfg);

        when(cctx.cacheObjectContext()).thenReturn(coctx);
        when(cctx.gridConfig()).thenReturn(cfg);

        when(cctx.grid()).thenReturn(ignite);
        when(kctx.grid()).thenReturn(ignite);

        when(ignite.configuration()).thenReturn(cfg);

        when(ctx.kernalContext()).thenReturn(kctx);
        when(cctx.kernalContext()).thenReturn(kctx);
        when(coctx.kernalContext()).thenReturn(kctx);

        when(coctx.binaryEnabled()).thenReturn(true);

        when(kctx.cache()).thenReturn(proc);
        when(kctx.config()).thenReturn(cfg);

        when(proc.context()).thenReturn(ctx);

        IgniteCacheObjectProcessor binaryProcessor = new CacheObjectBinaryProcessorImpl(kctx);
        when(kctx.cacheObjects()).thenReturn(binaryProcessor);

        // init marshaller
        marsh.setContext(new MarshallerContextTestImpl());
        BinaryContext bctx =
            new BinaryContext(BinaryCachingMetadataHandler.create(), new IgniteConfiguration(), new NullLogger());

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", bctx, new IgniteConfiguration());
    }

    /**
     * @param m Message.
     * @return Unmarshalled message.
     */
    protected <T extends GridCacheMessage> T marshalUnmarshal(T m) throws IgniteCheckedException {
        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);

        m.prepareMarshal(ctx);
        m.writeTo(buf, new DirectMessageWriter(proto));

        System.out.println("Binary size: " + buf.position() + " bytes");
        buf.flip();

        byte type = buf.get();
        assertEquals(m.directType(), type);

        MessageFactory msgFactory = new GridIoMessageFactory(null);

        Message mx = msgFactory.create(type);
        mx.readFrom(buf, new DirectMessageReader(msgFactory, proto));
        ((GridCacheMessage)mx).finishUnmarshal(ctx, U.gridClassLoader());

        return (T)mx;
    }

    /** */
    protected KeyCacheObject key(Object val, int part) {
        return new KeyCacheObjectImpl(val, null, part);
    }

    protected CacheObject val(Object val) {
        return new CacheObjectImpl(val, null);
    }
}
