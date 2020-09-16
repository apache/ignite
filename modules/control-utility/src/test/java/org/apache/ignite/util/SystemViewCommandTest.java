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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.SystemViewRowAttributeWalker;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.SYSTEM_VIEW;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.toSqlName;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests output of {@link CommandList#SYSTEM_VIEW} command.
 */
public class SystemViewCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Command line argument for printing content of a system view. */
    private static final String CMD_SYS_VIEW = SYSTEM_VIEW.text();

    /** Name of the test system view. */
    private static final String TEST_VIEW = "test.testView";

    /** Expected command output in case system view comtains no rows. */
    private static final String EXP_NO_ROWS_CMD_OUT = "boolean    char    byte    short    int    long    float" +
        "    double    date    uuid    igniteUUID    class    enum    object";

    /** Expected command output. */
    private static final String EXP_CMD_OUT;

    static {
        try {
            EXP_CMD_OUT = U.readFileToString(resolveIgnitePath(
                "modules/control-utility/src/test/resources/system-view-command.output").getAbsolutePath(), UTF_8.name());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < SERVER_NODE_CNT; i++) {
            ignite(i).context().systemView().registerView(
                TEST_VIEW,
                "test_view_desc",
                new TestViewWalker(),
                i == 0 ? () -> Arrays.asList(false, true) : Collections::emptyList,
                TestView::new
            );
        }
    }

    /** {@inheritDoc} */
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
        checkCommandExecution(EXIT_CODE_INVALID_ARGUMENTS,
            "The name of the system view for which its content should be printed is expected.",
            CMD_SYS_VIEW);
    }

    /**
     * Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is omitted.
     */
    @Test
    public void testNodeIdMissedFailure() {
        checkCommandExecution(EXIT_CODE_INVALID_ARGUMENTS,
            "ID of the node from which system view content should be obtained is expected.",
            CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName());
    }

    /**
     * Tests command error output in case value of {@link SystemViewCommandArg#NODE_ID} argument is invalid.
     */
    @Test
    public void testInvalidNodeIdFailure() {
        checkCommandExecution(EXIT_CODE_INVALID_ARGUMENTS,
            "Failed to parse " + NODE_ID.argName() +
            " command argument. String representation of \"java.util.UUID\" is exepected." +
            " For example: 123e4567-e89b-42d3-a456-556642440000",
            CMD_SYS_VIEW, SVCS_VIEW, NODE_ID.argName(), "invalid_node_id");
    }

    /**
     * Tests command error output in case multiple system view names are specified.
     */
    @Test
    public void testMultipleSystemViewNamesFailure() {
        checkCommandExecution(EXIT_CODE_INVALID_ARGUMENTS,
            "Multiple system view names are not supported.",
            CMD_SYS_VIEW, SVCS_VIEW, CACHE_GRP_PAGE_LIST_VIEW);
    }

    /**
     * Tests command error output in case {@link SystemViewCommandArg#NODE_ID} argument value refers to nonexistent
     * node.
     */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        checkCommandExecution(EXIT_CODE_INVALID_ARGUMENTS,
            "Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found",
            CMD_SYS_VIEW, "--node-id", incorrectNodeId, CACHES_VIEW);
    }

    /**
     * Tests command output in case nonexistent system view names is specified.
     */
    @Test
    public void testNonExistentSystemView() {
        checkCommandExecution(EXIT_CODE_OK,
            "No system view with specified name was found [name=non_existent_system_view]",
            CMD_SYS_VIEW, "non_existent_system_view");
    }

    /**
     * Tests command output.
     */
    @Test
    public void testSystemViewContentOutput() {
        checkCommandExecution(EXIT_CODE_OK, EXP_CMD_OUT, CMD_SYS_VIEW, TEST_VIEW);
    }

    /**
     * Tests command output in case correct {@link SystemViewCommandArg#NODE_ID} argument value is specified.
     */
    @Test
    public void testNodeIdArgument() {
        checkCommandExecution(EXIT_CODE_OK, EXP_CMD_OUT,
            CMD_SYS_VIEW, "--node-id", nodeId(0).toString(), TEST_VIEW);

        checkCommandExecution(EXIT_CODE_OK, EXP_NO_ROWS_CMD_OUT,
            CMD_SYS_VIEW, TEST_VIEW, "--node-id", nodeId(1).toString());
    }

    /**
     * Tests command output in case system view name specified in "SQL" style.
     */
    @Test
    public void testSqlStyleSystemViewName() {
        checkCommandExecution(EXIT_CODE_OK, EXP_CMD_OUT, CMD_SYS_VIEW, toSqlName(TEST_VIEW));
    }

    /**
     * Check that command execution exits with specified code and output.
     *
     * @param expOutput Expected command output.
     * @param args Command lines arguments.
     */
    private void checkCommandExecution(int exitCode, String expOutput, String... args) {
        int res = execute(args);

        assertEquals(exitCode, res);

        assertContains(log, testOut.toString(), expOutput);
    }

    /** */
    private static class TestView {
        static {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        }

        /** */
        private final boolean isLongOut;

        /** */
        public TestView(boolean isLongOut) {
            this.isLongOut = isLongOut;
        }

        /** */
        public boolean booleanValue() {
            return false;
        }

        /** */
        public char charValue() {
            return 'c';
        }

        /** */
        public byte byteValue() {
            return isLongOut ? Byte.MAX_VALUE : (byte)0;
        }

        /** */
        public short shortValue() {
            return isLongOut ? Short.MAX_VALUE : (short)0;
        }

        /** */
        public int intValue() {
            return isLongOut ? Integer.MAX_VALUE : 0;
        }

        /** */
        public long longValue() {
            return isLongOut ? Long.MAX_VALUE : 0L;
        }

        /** */
        public float floatValue() {
            return isLongOut ? Float.MAX_VALUE : 0F;
        }

        /** */
        public double doubleValue() {
            return isLongOut ? Double.MAX_VALUE : 0D;
        }

        /** */
        public Date dateValue() {
            return new Date(0);
        }

        /** */
        public UUID uuidValue() {
            return UUID.fromString("364eef57-54e7-4a0f-a48b-3becbc25517e");
        }

        /** */
        public IgniteUuid igniteUuidValue() {
            return IgniteUuid.fromString("0c891f79471-2d5ec79e-b712-4c73-9111-3f80ab5e2893");
        }

        /** */
        public Class<?> classValue() {
            return Object.class;
        }

        /** */
        public CacheMode enumValue() {
            return CacheMode.PARTITIONED;
        }

        /** */
        public Object objectValue() {
            return isLongOut ?
                new Object() {
                    @Override public String toString() {
                        return "custom object";
                    }
                } : null;
        }
    }

    /** */
    private static class TestViewWalker implements SystemViewRowAttributeWalker<TestView> {
        /** {@inheritDoc} */
        @Override public void visitAll(AttributeVisitor v) {
            v.accept(0, "boolean", boolean.class);
            v.accept(1, "char", char.class);
            v.accept(2, "byte", byte.class);
            v.accept(3, "short", short.class);
            v.accept(4, "int", int.class);
            v.accept(5, "long", long.class);
            v.accept(6, "float", float.class);
            v.accept(7, "double", double.class);
            v.accept(8, "date", Date.class);
            v.accept(9, "uuid", UUID.class);
            v.accept(10, "igniteUUID", IgniteUuid.class);
            v.accept(11, "class", Class.class);
            v.accept(12, "enum", CacheMode.class);
            v.accept(13, "object", Object.class);
        }

        /** {@inheritDoc} */
        @Override public void visitAll(TestView row, AttributeWithValueVisitor v) {
            v.acceptBoolean(0, "boolean", row.booleanValue());
            v.acceptChar(1, "char", row.charValue());
            v.acceptByte(2, "byte", row.byteValue());
            v.acceptShort(3, "short", row.shortValue());
            v.acceptInt(4, "int", row.intValue());
            v.acceptLong(5, "long", row.longValue());
            v.acceptFloat(6, "float", row.floatValue());
            v.acceptDouble(7, "double", row.doubleValue());
            v.accept(8, "date", Date.class, row.dateValue());
            v.accept(9, "uuid", UUID.class, row.uuidValue());
            v.accept(10, "igniteUUID", IgniteUuid.class, row.igniteUuidValue());
            v.accept(11, "class", Class.class, row.classValue());
            v.accept(12, "enum", CacheMode.class, row.enumValue());
            v.accept(13, "object", Object.class, row.objectValue());
        }

        /** {@inheritDoc} */
        @Override public int count() {
            return 14;
        }
    }
}
