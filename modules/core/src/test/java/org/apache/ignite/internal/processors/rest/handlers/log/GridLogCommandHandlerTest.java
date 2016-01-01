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
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class GridLogCommandHandlerTest extends GridCommonAbstractTest {

    public void testSupportedCommands() throws Exception {
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(super.newContext());
        Collection<GridRestCommand> commandCollection = gridLogCommandHandler.supportedCommands();
        assertTrue(commandCollection.contains(GridRestCommand.LOG));
    }

    public void testUnSupportedCommands() throws Exception {
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(super.newContext());
        Collection<GridRestCommand> commandCollection = gridLogCommandHandler.supportedCommands();
        assertFalse(commandCollection.contains(GridRestCommand.VERSION));
    }

    public void testHandleAsync() throws Exception {
        GridTestKernalContext ctx = super.newContext();
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest gridLogRestRequest = new GridRestLogRequest();
        gridLogRestRequest.to(5);
        gridLogRestRequest.from(2);
        gridLogRestRequest.path(getClass().getResource("/Test").getFile());
        GridRestRequest gridRestRequest = (GridRestLogRequest) gridLogRestRequest;
        gridRestRequest.command(GridRestCommand.LOG);
        IgniteInternalFuture<GridRestResponse> gridRestResponse = gridLogCommandHandler.handleAsync(gridRestRequest);
        assertEquals(0, gridRestResponse.result().getSuccessStatus());
        assertNotNull(gridRestResponse.result().getResponse());
        assertEquals(null, gridRestResponse.result().getError());
    }

    public void testHandleAsyncFromAndToNotSet() throws Exception {
        GridTestKernalContext ctx = super.newContext();
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest gridLogRestRequest = new GridRestLogRequest();
        gridLogRestRequest.path(getClass().getResource("/Test").getFile());
        GridRestRequest gridRestRequest = (GridRestLogRequest) gridLogRestRequest;
        gridRestRequest.command(GridRestCommand.LOG);
        IgniteInternalFuture<GridRestResponse> gridRestResponse = gridLogCommandHandler.handleAsync(gridRestRequest);
        assertEquals(0, gridRestResponse.result().getSuccessStatus());
        assertNotNull(gridRestResponse.result().getResponse());
        assertEquals(null, gridRestResponse.result().getError());
    }

    public void testHandleAsyncPathNotSet() throws Exception {
        GridTestKernalContext ctx = super.newContext();
        ctx.config().setIgniteHome(getClass().getResource("/").getFile());
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest gridLogRestRequest = new GridRestLogRequest();
        gridLogRestRequest.to(5);
        gridLogRestRequest.from(2);
        GridRestRequest gridRestRequest = (GridRestLogRequest) gridLogRestRequest;
        gridRestRequest.command(GridRestCommand.LOG);
        IgniteInternalFuture<GridRestResponse> gridRestResponse = gridLogCommandHandler.handleAsync(gridRestRequest);
        assertEquals(0, gridRestResponse.result().getSuccessStatus());
        assertNotNull(gridRestResponse.result().getResponse());
        assertEquals(null, gridRestResponse.result().getError());
    }

    public void testHandleAsyncFromGreaterThanTo() throws Exception {
        GridTestKernalContext ctx = super.newContext();
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest gridLogRestRequest = new GridRestLogRequest();
        gridLogRestRequest.to(2);
        gridLogRestRequest.from(5);
        gridLogRestRequest.path(getClass().getResource("/Test").getFile());
        GridRestRequest gridRestRequest = (GridRestLogRequest) gridLogRestRequest;
        gridRestRequest.command(GridRestCommand.LOG);
        IgniteInternalFuture<GridRestResponse> gridRestResponse = gridLogCommandHandler.handleAsync(gridRestRequest);
        assertEquals(1, gridRestResponse.result().getSuccessStatus());
        assertNull(gridRestResponse.result().getResponse());
        assertEquals("Log file start from cannot be greater than to", gridRestResponse.result().getError());
    }

    public void testHandleAsyncFromEqualTo() throws Exception {
        GridTestKernalContext ctx = super.newContext();
        GridLogCommandHandler gridLogCommandHandler = new GridLogCommandHandler(ctx);
        GridRestLogRequest gridLogRestRequest = new GridRestLogRequest();
        gridLogRestRequest.to(2);
        gridLogRestRequest.from(2);
        gridLogRestRequest.path(getClass().getResource("/Test").getFile());
        GridRestRequest gridRestRequest = (GridRestLogRequest) gridLogRestRequest;
        gridRestRequest.command(GridRestCommand.LOG);
        IgniteInternalFuture<GridRestResponse> gridRestResponse = gridLogCommandHandler.handleAsync(gridRestRequest);
        assertEquals(1, gridRestResponse.result().getSuccessStatus());
        assertNull(gridRestResponse.result().getResponse());
        assertEquals("Log file start from cannot be greater than to", gridRestResponse.result().getError());
    }
}
