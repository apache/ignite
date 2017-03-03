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

package org.apache.ignite.internal.processors.query.h2.ddl;

import java.io.Serializable;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.ddl.msg.DdlInitDiscoveryMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridDdlProtoTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteH2Indexing.ddlProcCls = DdlProc.class;

        startGridsMultiThreaded(3, true);

        ignite(0).createCache(cacheConfig("S2P", true, false).setIndexedTypes(String.class, Person.class));

        startGrid(getTestGridName(3), getConfiguration().setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        DdlProc.testName = null;

        ignite(0).cache("S2P").clear();

        ignite(0).cache("S2P").put("FirstKey", new Person(1, "John", "White"));
        ignite(0).cache("S2P").put("SecondKey", new Person(2, "Joe", "Black"));
        ignite(0).cache("S2P").put("k3", new Person(3, "Sylvia", "Green"));
        ignite(0).cache("S2P").put("f0u4thk3y", new Person(4, "Jane", "Silver"));
    }

    /** Test behavior in case of INIT failure (cancel via {@link DdlInitDiscoveryMessage#ackMessage}). */
    public void testInitFailure() {
        DdlProc.testName = GridTestUtils.getGridTestName();

        assertCreateIndexThrowsWithMessage("DDL operation has been cancelled at INIT stage", false);
    }

    /**
     * Test error handling.
     *
     * @param msg Expected message.
     * @param loc Run query locally on single node.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertCreateIndexThrowsWithMessage(String msg, final boolean loc) {
        final Throwable e = GridTestUtils.assertThrows(null, new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                ignite(3).cache("S2P").query(new SqlFieldsQuery("create index idx on Person(id desc)").setLocal(loc));
                return null;
            }
        }, IgniteSQLException.class, "Failed to execute DDL statement");

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                throw (Exception) e.getCause();
            }
        }, IgniteCheckedException.class, msg);
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param escapeSql whether identifiers should be quoted - see {@link CacheConfiguration#setSqlEscapeAll}
     * @return Cache configuration.
     */
    protected static CacheConfiguration cacheConfig(String name, boolean partitioned, boolean escapeSql) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setSqlEscapeAll(escapeSql);
    }
    
    /**
     *
     */
    static class Person implements Serializable {
        /** */
        public Person(int id, String name, String secondName) {
            this.id = id;
            this.name = name;
            this.secondName = secondName;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField(name = "firstName")
        protected final String name;

        /** */
        @QuerySqlField
        final String secondName;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            return id == person.id && name.equals(person.name) && secondName.equals(person.secondName);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = id;
            res = 31 * res + name.hashCode();
            res = 31 * res + secondName.hashCode();
            return res;
        }
    }

    /**
     * Custom implementation to test behavior on failure during various stages.
     */
    public final static class DdlProc extends DdlStatementsProcessor {
        /** Name of current test. */
        private static volatile String testName;

        /** {@inheritDoc} */
        @Override void doInit(DdlAbstractOperation args) {
            // Let's throw an exception on a single node in the ring
            if ("InitFailure".equals(testName) && ctx.gridName().endsWith("2"))
                throw new RuntimeException("Hello from DdlProc Init");
            else
                try {
                    super.doInit(args);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
        }

        /** {@inheritDoc}
         * @param args*/
        @Override void doAck(DdlAbstractOperation args) {
            if ("AckFailure".equals(testName) && ctx.gridName().endsWith("1"))
                throw new RuntimeException("Hello from DdlProc Ack");
            else
                try {
                    super.doInit(args);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
        }
    }
}
