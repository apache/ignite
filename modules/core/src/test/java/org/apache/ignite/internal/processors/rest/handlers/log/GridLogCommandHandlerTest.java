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
package org.apache.ignite.internal.processors.rest.handlers.log;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestLogRequest;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * REST log command handler tests.
 */
public class GridLogCommandHandlerTest extends GridCommonAbstractTest {
    /** */
    private String igniteHome = System.getProperty("user.dir");

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        List<String> lines = Arrays.asList("[22:01:30,329][INFO ][grid-load-test-thread-12][GridDeploymentLocalStore] ",
            "[22:01:30,329][INFO ][grid-load-test-thread-18][GridDeploymentLocalStore] Removed undeployed class: \n",
            "[22:01:30,329][INFO ][grid-load-test-thread-18][GridDeploymentLocalStore] Task locally undeployed: \n"
        );

        Path dir = Paths.get(igniteHome + "/work/log");
        Files.createDirectories(dir);

        Path file = Paths.get(igniteHome + "/work/log/" + "ignite.log");
        Files.write(file, lines, Charset.forName("UTF-8"));

        file = Paths.get(igniteHome + "/work/log/" + "test.log");
        Files.write(file, lines, Charset.forName("UTF-8"));

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        Files.delete(Paths.get(igniteHome + "/work/log/" + "test.log"));
        Files.delete(Paths.get(igniteHome + "/work/log/" + "ignite.log"));
        Files.delete(Paths.get(igniteHome + "/work/log/"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSupportedCommands() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());

        Collection<GridRestCommand> commands = cmdHandler.supportedCommands();

        assertEquals(1, commands.size());
        assertTrue(commands.contains(GridRestCommand.LOG));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnSupportedCommands() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());

        Collection<GridRestCommand> commands = cmdHandler.supportedCommands();

        assertEquals(1, commands.size());
        assertFalse(commands.contains(GridRestCommand.VERSION));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsync() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(2);

        req.path(igniteHome + "/work/log/" + "test.log");
        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncForNonExistingLines() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(50);
        req.from(20);

        req.path(igniteHome + "/work/log/" + "test.log");
        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'from' and 'to' are for lines that do not exist in log file.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncFromAndToNotSet() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.path(igniteHome + "/work/log/" + "test.log");
        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncPathNotSet() throws Exception {
        GridTestKernalContext ctx = newContext();
        ctx.config().setIgniteHome(igniteHome);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(2);

        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncPathIsOutsideIgniteHome() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(2);
        req.path("/home/users/mytest.log");

        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'path' must contain a path to valid log file.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncFromGreaterThanTo() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(7);

        req.path(igniteHome + "/work/log/" + "test.log");
        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'from' must be less than 'to'.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHandleAsyncFromEqualTo() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setIgniteHome(igniteHome);
        GridTestKernalContext ctx = newContext(cfg);
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(5);

        req.path(igniteHome + "/work/log/" + "test.log");
        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'from' must be less than 'to'.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }
}
