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

package org.apache.ignite.marshaller;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import javax.cache.event.EventType;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.Files.readAllBytes;

/**
 * Test marshaller context.
 */
public class MarshallerContextSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testClassName() throws Exception {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false);

        final MarshallerContextImpl.ContinuousQueryListener queryListener =
                new MarshallerContextImpl.ContinuousQueryListener(log, workDir);

        final ArrayList evts = new ArrayList<>();

        IgniteCacheProxy cache = new IgniteCacheProxy();

        evts.add(new CacheContinuousQueryManager.CacheEntryEventImpl(cache,
            EventType.CREATED,
            1,
            String.class.getName()));

        queryListener.onUpdated(evts);

        try (Ignite g1 = startGrid(1)) {
            MarshallerContextImpl marshCtx = ((IgniteKernal)g1).context().marshallerContext();
            String clsName = marshCtx.className(1);

            assertEquals("java.lang.String", clsName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnUpdated() throws Exception {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false);

        final MarshallerContextImpl.ContinuousQueryListener queryListener =
                new MarshallerContextImpl.ContinuousQueryListener(log, workDir);

        final ArrayList evts = new ArrayList<>();

        IgniteCacheProxy cache = new IgniteCacheProxy();

        evts.add(new CacheContinuousQueryManager.CacheEntryEventImpl(cache,
            EventType.CREATED,
            1,
            String.class.getName()));

        queryListener.onUpdated(evts);

        String fileName = "1.classname";

        assertEquals("java.lang.String", new String(readAllBytes(Paths.get(workDir + "/" + fileName))));
    }
}
