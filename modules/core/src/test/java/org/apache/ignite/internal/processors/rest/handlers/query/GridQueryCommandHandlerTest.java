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

package org.apache.ignite.internal.processors.rest.handlers.query;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.request.RestQueryRequest;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * REST query command handler tests.
 */
public class GridQueryCommandHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();

        ConnectorConfiguration connCfg = new ConnectorConfiguration();

        connCfg.setIdleQueryCursorCheckFrequency(1000);
        connCfg.setIdleQueryCursorTimeout(1000);

        grid().configuration().setConnectorConfiguration(connCfg);

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSupportedCommands() throws Exception {
        GridTestKernalContext ctx = newContext(grid().configuration());

        ctx.add(new GridTimeoutProcessor(ctx));

        QueryCommandHandler cmdHnd = new QueryCommandHandler(ctx);

        Collection<GridRestCommand> commands = cmdHnd.supportedCommands();

        assertEquals(5, commands.size());

        assertTrue(commands.contains(GridRestCommand.EXECUTE_SQL_QUERY));
        assertTrue(commands.contains(GridRestCommand.EXECUTE_SQL_FIELDS_QUERY));
        assertTrue(commands.contains(GridRestCommand.EXECUTE_SCAN_QUERY));
        assertTrue(commands.contains(GridRestCommand.FETCH_SQL_QUERY));
        assertTrue(commands.contains(GridRestCommand.CLOSE_SQL_QUERY));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnsupportedCommands() throws Exception {
        GridTestKernalContext ctx = newContext(grid().configuration());

        ctx.add(new GridTimeoutProcessor(ctx));

        QueryCommandHandler cmdHnd = new QueryCommandHandler(ctx);

        Collection<GridRestCommand> commands = cmdHnd.supportedCommands();

        assertFalse(commands.contains(GridRestCommand.LOG));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullCache() throws Exception {
        QueryCommandHandler cmdHnd = new QueryCommandHandler(grid().context());

        Integer arg1 = 1000;

        Object[] arr = new Object[] {arg1, arg1};

        RestQueryRequest req = new RestQueryRequest();

        req.command(GridRestCommand.EXECUTE_SQL_QUERY);
        req.queryType(RestQueryRequest.QueryType.SCAN);
        req.typeName(Integer.class.getName());
        req.pageSize(10);
        req.sqlQuery("salary+>+%3F+and+salary+<%3D+%3F");
        req.arguments(arr);
        req.cacheName(null);

        IgniteInternalFuture<GridRestResponse> resp = cmdHnd.handleAsync(req);
        resp.get();

        // If cache name is not set server uses name 'default'.
        assertEquals("Failed to find cache with name: default", resp.result().getError());
        assertEquals(GridRestResponse.STATUS_FAILED, resp.result().getSuccessStatus());
        assertNull(resp.result().getResponse());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullPageSize() throws Exception {
        grid().getOrCreateCache(getName());

        QueryCommandHandler cmdHnd = new QueryCommandHandler(grid().context());

        Integer arg1 = 1000;

        Object[] arr = new Object[] {arg1, arg1};

        RestQueryRequest req = new RestQueryRequest();

        req.command(GridRestCommand.EXECUTE_SQL_QUERY);
        req.queryType(RestQueryRequest.QueryType.SCAN);
        req.typeName(Integer.class.getName());

        req.pageSize(null);
        req.sqlQuery("salary+>+%3F+and+salary+<%3D+%3F");

        req.arguments(arr);
        req.cacheName(getName());

        try {
            IgniteInternalFuture<GridRestResponse> resp = cmdHnd.handleAsync(req);
            resp.get();

            fail("Expected exception not thrown.");
        }
        catch (IgniteCheckedException e) {
            info("Got expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQuery() throws Exception {
        grid().getOrCreateCache(getName());

        QueryCommandHandler cmdHnd = new QueryCommandHandler(grid().context());

        Integer arg1 = 1000;

        Object[] arr = new Object[] {arg1, arg1};

        RestQueryRequest req = new RestQueryRequest();

        req.command(GridRestCommand.EXECUTE_SQL_QUERY);
        req.queryType(RestQueryRequest.QueryType.SCAN);
        req.typeName(Integer.class.getName());
        req.pageSize(null);
        req.sqlQuery("salary+>+%3F+and+salary+<%3D+%3F");
        req.arguments(arr);
        req.cacheName(getName());
        req.pageSize(10);

        IgniteInternalFuture<GridRestResponse> resp = cmdHnd.handleAsync(req);
        resp.get();

        assertNull(resp.result().getError());
        assertEquals(GridRestResponse.STATUS_SUCCESS, resp.result().getSuccessStatus());
        assertNotNull(resp.result().getResponse());

        CacheQueryResult res = (CacheQueryResult) resp.result().getResponse();

        assertTrue(res.getLast());
    }
}
