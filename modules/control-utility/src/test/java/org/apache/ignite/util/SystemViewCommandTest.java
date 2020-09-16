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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker.AttributeVisitor;
import org.junit.Test;

import static java.util.regex.Pattern.quote;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommand.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;
import static org.apache.ignite.internal.processors.query.RunningQueryManager.SQL_QRY_HIST_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests output of {@link CommandList#SYSTEM_VIEW} command.
 */
public class SystemViewCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /**
     * Line in command output that precedes the system view content.
     */
    static final String SYS_VIEW_OUTPUT_START =
        "--------------------------------------------------------------------------------";

    /**
     * Line in command output that indicates end of the system view content.
     */
    static final String SYS_VIEW_OTPUT_END =
        "Command [" + SYSTEM_VIEW.toCommandName() + "] finished with code: " + EXIT_CODE_OK;
    
    /** */
    private static final String CMD_SYS_VIEW = SYSTEM_VIEW.text();

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();

        autoConfirmation = false;
    }

    /**
     * Tests command error output in case of mandatory system view name is omitted.
     */
    @Test
    public void testSystemViewNameMissedFailure() {
        assertCommandFails("The name of the system view for which its content should be printed is expected.",
            CMD_SYS_VIEW);
    }

    /**
     * Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is omitted.
     */
    @Test
    public void testNodeIdMissedFailure() {
        assertCommandFails("ID of the node from which system view content should be obtained is expected.",
            CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName());
    }

    /**
     * Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is invalid.
     */
    @Test
    public void testInvalidNodeIdFailure() {
        assertCommandFails("Failed to parse " + NODE_ID.argName() +
            " command argument. String representation of \"java.util.UUID\" is exepected." +
            " For example: 123e4567-e89b-42d3-a456-556642440000",
            CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName(), "invalid_node_id");
    }

    /**
     * Tests command error output in case multiple system view names are specified.
     */
    @Test
    public void testMultipleSystemViewNamesFailure() {
        assertCommandFails("Multiple system view names are not supported.",
            CMD_SYS_VIEW, SVCS_VIEW, CACHE_GRP_PAGE_LIST_VIEW);
    }

    /**
     * Tests command error output in case {@link SystemViewCommandArg#NODE_ID} argument value refers to nonexistent
     * node.
     */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        assertCommandFails("Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found",
            CMD_SYS_VIEW, "--node-id", incorrectNodeId, CACHES_VIEW);
    }

    /**
     * Tests command output in case nonexistent system view names is specified.
     */
    @Test
    public void testNonExistentSystemView() {
        assertCommandSucceed(CMD_SYS_VIEW, "non_existent_system_view");

        assertContains(log, testOut.toString(),
            "No system view with specified name was found [name=non_existent_system_view]");
    }

    /**
     * Tests command output.
     */
    @Test
    public void testSystemViewContentPrinting() {
        ignite(0).createCache("default");

        assertCommandSucceed(CMD_SYS_VIEW, CACHES_VIEW);

        checkSystemViewContentOutput(CACHES_VIEW, 0, 2);
    }

    /**
     * Tests command output in case correct {@link SystemViewCommandArg#NODE_ID} argument value is specified.
     */
    @Test
    public void testNodeIdArgument() {
        grid(0).context().query().querySqlFields(new SqlFieldsQuery(
            "SELECT * FROM SYS.SCHEMAS"), false).getAll();

        assertCommandSucceed(CMD_SYS_VIEW, "--node-id", nodeId(0).toString(), SQL_QRY_HIST_VIEW);

        checkSystemViewContentOutput(SQL_QRY_HIST_VIEW, 0, 1);

        assertCommandSucceed(CMD_SYS_VIEW, SQL_QRY_HIST_VIEW, "--node-id", nodeId(1).toString());

        checkSystemViewContentOutput(SQL_QRY_HIST_VIEW, 1, 0);
    }

    /**
     * Tests command output in case system view name specified in "SQL" style.
     */
    @Test
    public void testSqlStyleSystemViewName() {
        assertCommandSucceed(CMD_SYS_VIEW, toSqlName(CACHE_GRPS_VIEW));

        checkSystemViewContentOutput(CACHE_GRPS_VIEW, 0, 1);
    }

    /**
     * Checks output of {@link CommandList#SYSTEM_VIEW} command.
     *
     * @param sysViewName    Name of requested system view.
     * @param nodeIdx        Index of node from which system view content was requested.
     * @param expSysViewRows Expected number of system view rows in output.
     */
    private void checkSystemViewContentOutput(String sysViewName, int nodeIdx, int expSysViewRows) {
        List<String> attrNames = new ArrayList<>();

        SystemView<Object> sysView = ignite(nodeIdx).context().systemView().view(sysViewName);

        sysView.walker().visitAll(new AttributeVisitor() {
            @Override public <T> void accept(int idx, String name, Class<T> clazz) {
                attrNames.add(name);
            }
        });

        String out = testOut.toString();

        List<String> cmdRows = Arrays.asList(out.substring(
            out.indexOf(SYS_VIEW_OUTPUT_START) + SYS_VIEW_OUTPUT_START.length() + 1,
            out.indexOf(SYS_VIEW_OTPUT_END)
        ).split(U.nl()));

        assertEquals(expSysViewRows + 1, cmdRows.size());

        assertEquals(attrNames, splitRow(cmdRows.get(0)));

        for (int rowIdx = 1; rowIdx < cmdRows.size(); rowIdx++)
            assertEquals(sysView.walker().count(), splitRow(cmdRows.get(rowIdx)).size());
    }

    /**
     * Asserts that command execution succeed.
     *
     * @param args Command lines arguments.
     */
    private void assertCommandSucceed(String... args) {
        int res = execute(args);

        assertEquals(EXIT_CODE_OK, res);
    }

    /**
     * Asserts that command execution fails and specified message appears in the output.
     *
     * @param msg Error message.
     * @param args Command line arguments.
     */
    private void assertCommandFails(String msg, String... args) {
        int res = execute(args);

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, res);

        assertContains(log, testOut.toString(), msg);
    }

    /**
     * Splits {@link String} row representation into separate column values.
     *
     * @param row Row to split up.
     * @return Column values.
     */
    private List<String> splitRow(String row) {
        return Arrays.stream(row.split(quote(COLUMN_SEPARATOR)))
            .map(String::trim)
            .filter(str -> !str.isEmpty())
            .collect(Collectors.toList());
    }
}
