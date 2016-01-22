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

import java.util.Collection;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.GridRestLogRequest;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * REST log command handler tests.
 */
public class GridLogCommandHandlerTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSupportedCommands() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());

        Collection<GridRestCommand> commands = cmdHandler.supportedCommands();

        assertEquals(1, commands.size());
        assertTrue(commands.contains(GridRestCommand.LOG));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnSupportedCommands() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());

        Collection<GridRestCommand> commands = cmdHandler.supportedCommands();

        assertEquals(1, commands.size());
        assertFalse(commands.contains(GridRestCommand.VERSION));
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandleAsync() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(5);
        req.from(2);

        req.path(getClass().getResource("/test.log").getFile());

        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandleAsyncFromAndToNotSet() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());
        GridRestLogRequest req = new GridRestLogRequest();

        req.path(getClass().getResource("/test.log").getFile());

        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandleAsyncPathNotSet() throws Exception {
        GridTestKernalContext ctx = newContext();
        ctx.config().setIgniteHome(getClass().getResource("/").getFile());

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
    public void testHandleAsyncFromGreaterThanTo() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(2);
        req.from(5);
        req.path(getClass().getResource("/test.log").getFile());


        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'from' must be less than 'to'.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandleAsyncFromEqualTo() throws Exception {
        GridLogCommandHandler cmdHandler = new GridLogCommandHandler(newContext());
        GridRestLogRequest req = new GridRestLogRequest();

        req.to(2);
        req.from(2);
        req.path(getClass().getResource("/test.log").getFile());

        IgniteInternalFuture<GridRestResponse> resp = cmdHandler.handleAsync(req);

        assertEquals("Request parameter 'from' must be less than 'to'.", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }
}
