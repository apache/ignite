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
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.Files.readAllBytes;
import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Test marshaller context.
 */
public class MarshallerContextSelfTest extends GridCommonAbstractTest {

    private GridTestKernalContext ctx;

    @Override
    protected void beforeTest() throws Exception {
        ctx = newContext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassName() throws Exception {
        MarshallerContextImpl marshallerContext = new MarshallerContextImpl(null);

        marshallerContext.onMarshallerProcessorStarted(ctx);

        MarshallerMappingItem item = new MarshallerMappingItem();

        item.setPlatformId(JAVA_ID);
        item.setTypeId(1);
        item.setClassName(String.class.getName());

        marshallerContext.onMappingAccepted(item);

        try (Ignite g1 = startGrid(1)) {
            MarshallerContextImpl marshCtx = ((IgniteKernal)g1).context().marshallerContext();
            String clsName = marshCtx.getClassName(JAVA_ID, 1);

            assertEquals("java.lang.String", clsName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnUpdated() throws Exception {
        File workDir = U.resolveWorkDirectory("marshaller", false);
        MarshallerContextImpl context = new MarshallerContextImpl(null);

        context.onMarshallerProcessorStarted(ctx);

        MarshallerMappingItem item = new MarshallerMappingItem();
        item.setTypeId(1);
        item.setPlatformId(JAVA_ID);
        item.setClassName(String.class.getName());

        context.onMappingAccepted(item);

        String fileName = "1.classname0";

        assertEquals("java.lang.String", new String(readAllBytes(Paths.get(workDir + "/" + fileName))));
    }
}
