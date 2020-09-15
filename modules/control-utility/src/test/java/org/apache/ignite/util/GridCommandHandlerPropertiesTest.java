/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedChangeableProperty;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks command line property commands.
 */
public class GridCommandHandlerPropertiesTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** */
    @After
    public void clear() {
    }

    /**
     * Check the command '--property list'.
     * Steps:
     */
    @Test
    public void testList() {
        assertEquals(EXIT_CODE_OK, execute("--property", "list"));

        String out = testOut.toString();

        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties())
            assertContains(log, out, pd.getName());
    }

    /**
     * Check the command '--property get'.
     * Steps:
     */
    @Test
    public void testGet() {
        for (DistributedChangeableProperty<Serializable> pd : crd.context()
            .distributedConfiguration().properties()) {
            assertEquals(EXIT_CODE_OK, execute("--property", "get", "--name", pd.getName()));
            String out = testOut.toString();

            assertContains(log, out, pd.getName() + " = " + pd.get());
        }
    }

    /**
     * Check the set command fro property 'sql.disabledFunctions'.
     * Steps:
     */
    @Test
    public void testPropertyDisabledSqlFunctions() {
        assertEquals(
            EXIT_CODE_OK,
            execute(
                "--property", "set",
                "--name", "sql.disabledFunctions",
                "--val", "LENGTH, SESSION_ID"
            )
        );

        for (Ignite ign : G.allGrids()) {
            Set<String> disabledFuncs = ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                .distributedConfiguration().disabledFunctions();

            assertEquals(2, disabledFuncs.size());

            assertTrue(disabledFuncs.contains("LENGTH"));
            assertTrue(disabledFuncs.contains("SESSION_ID"));
        }
    }

    /**
     * Check the set command fro property 'sql.defaultQueryTimeout'.
     * Steps:
     */
    @Test
    public void testPropertyDefaultQueryTimeout() {
        int dfltVal = ((IgniteH2Indexing)crd.context().query().getIndexing())
            .distributedConfiguration().defaultQueryTimeout();

        int newVal = dfltVal + 1000;

        assertEquals(EXIT_CODE_OK, execute("--property", "set", "--name", "sql.defaultQueryTimeout", "--val",
            Integer.toString(newVal)));

        for (Ignite ign : G.allGrids()) {
            assertEquals(
                "Invalid default query timeout on node: " + ign.name(),
                newVal,
                ((IgniteH2Indexing)((IgniteEx)ign).context().query().getIndexing())
                    .distributedConfiguration().defaultQueryTimeout()
            );
        }

        assertEquals(
            EXIT_CODE_INVALID_ARGUMENTS,
            execute(
                "--property", "set",
                "--name", "sql.defaultQueryTimeout",
                "--val", "invalidVal"
            )
        );
    }
}
