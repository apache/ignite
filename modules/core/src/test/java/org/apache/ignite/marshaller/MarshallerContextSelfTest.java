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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.file.Files.readAllBytes;
import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Test marshaller context.
 */
public class MarshallerContextSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTestKernalContext ctx;

    /** */
    private ExecutorService execSvc;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = newContext();
        execSvc = Executors.newSingleThreadExecutor();

        ctx.setSystemExecutorService(execSvc);

        ctx.add(new PoolProcessor(ctx));

        ctx.add(new GridClosureProcessor(ctx));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClassName() throws Exception {
        MarshallerContextImpl marshCtx = new MarshallerContextImpl(null, null);

        marshCtx.onMarshallerProcessorStarted(ctx, null);

        MarshallerMappingItem item = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        marshCtx.onMappingProposed(item);

        marshCtx.onMappingAccepted(item);

        try (Ignite g1 = startGrid(1)) {
            marshCtx = ((IgniteKernal)g1).context().marshallerContext();
            String clsName = marshCtx.getClassName(JAVA_ID, 1);

            assertEquals("java.lang.String", clsName);
        }
    }

    /**
     * Test for adding non-java mappings (with platformId &gt; 0) to MarshallerContext and collecting them
     * for discovery.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiplatformMappingsCollecting() throws Exception {
        String nonJavaClassName = "random.platform.Mapping";

        MarshallerContextImpl marshCtx = new MarshallerContextImpl(null, null);

        marshCtx.onMarshallerProcessorStarted(ctx, null);

        MarshallerMappingItem item = new MarshallerMappingItem((byte) 2, 101, nonJavaClassName);

        marshCtx.onMappingProposed(item);

        marshCtx.onMappingAccepted(item);

        ArrayList<Map<Integer, MappedName>> allMappings = marshCtx.getCachedMappings();

        assertEquals(allMappings.size(), 3);

        assertTrue(allMappings.get(0).isEmpty());

        assertTrue(allMappings.get(1).isEmpty());

        Map<Integer, MappedName> nonJavaMappings = allMappings.get(2);

        assertNotNull(nonJavaMappings);

        assertNotNull(nonJavaMappings.get(101));

        assertEquals(nonJavaClassName, nonJavaMappings.get(101).className());
    }

    /**
     * Test for adding non-java mappings (with platformId &gt; 0) to MarshallerContext and distributing them
     * to newly joining nodes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiplatformMappingsDistributing() throws Exception {
        String nonJavaClassName = "random.platform.Mapping";

        Ignite grid0 = startGrid(0);

        MarshallerContextImpl marshCtx0 = ((IgniteKernal)grid0).context().marshallerContext();

        MarshallerMappingItem item = new MarshallerMappingItem((byte) 2, 101, nonJavaClassName);

        marshCtx0.onMappingProposed(item);

        marshCtx0.onMappingAccepted(item);

        Ignite grid1 = startGrid(1);

        MarshallerContextImpl marshCtx1 = ((IgniteKernal)grid1).context().marshallerContext();

        assertEquals(nonJavaClassName, marshCtx1.getClassName((byte) 2, 101));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnUpdated() throws Exception {
        File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DataStorageConfiguration.DFLT_MARSHALLER_PATH, false);
        MarshallerContextImpl ctx = new MarshallerContextImpl(null, null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        // Wait until marshaller context write class to file.
        U.sleep(2_000);

        checkFileName("java.lang.String", Paths.get(workDir + "/1.classname0"));

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 2, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);
        ctx.onMappingAccepted(item2);

        execSvc.shutdown();

        if (execSvc.awaitTermination(1000, TimeUnit.MILLISECONDS))
            checkFileName("Random.Class.Name", Paths.get(workDir + "/2.classname2"));
        else
            fail("Failed to wait for executor service to shutdown");
    }

    /**
     * Tests that there is a null value inserted in allCaches list
     * if platform ids passed to marshaller cache were not sequential (like 0, 2).
     */
    @Test
    public void testCacheStructure0() throws Exception {
        MarshallerContextImpl ctx = new MarshallerContextImpl(null, null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 2, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);

        List list = U.field(ctx, "allCaches");

        assertNotNull("Mapping cache is null for platformId: 0", list.get(0));
        assertNull("Mapping cache is not null for platformId: 1", list.get(1));
        assertNotNull("Mapping cache is null for platformId: 2", list.get(2));

        boolean excObserved = false;
        try {
            list.get(3);
        }
        catch (ArrayIndexOutOfBoundsException ignored) {
            excObserved = true;
        }
        assertTrue("ArrayIndexOutOfBoundsException had to be thrown", excObserved);
    }

    /**
     * Tests that there are no null values in allCaches list
     * if platform ids passed to marshaller context were sequential.
     */
    @Test
    public void testCacheStructure1() throws Exception {
        MarshallerContextImpl ctx = new MarshallerContextImpl(null, null);

        ctx.onMarshallerProcessorStarted(this.ctx, null);

        MarshallerMappingItem item1 = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

        ctx.onMappingAccepted(item1);

        MarshallerMappingItem item2 = new MarshallerMappingItem((byte) 1, 2, "Random.Class.Name");

        ctx.onMappingProposed(item2);

        List list = U.field(ctx, "allCaches");

        assertNotNull("Mapping cache is null for platformId: 0", list.get(0));
        assertNotNull("Mapping cache is null for platformId: 1", list.get(1));

        boolean excObserved = false;

        try {
            list.get(2);
        }
        catch (ArrayIndexOutOfBoundsException ignored) {
            excObserved = true;
        }

        assertTrue("ArrayIndexOutOfBoundsException had to be thrown", excObserved);
    }

    /**
     * @param expected Expected.
     * @param pathToReal Path to real.
     */
    private void checkFileName(String expected, Path pathToReal) throws IOException {
        byte[] fileContent = readAllBytes(pathToReal);

        assert fileContent != null && fileContent.length > 0;

        assertEquals(expected, new String(fileContent));
    }
}
