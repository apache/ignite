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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.calcite.GridCommonAbstractWrapperTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.resources.SessionContextProviderResource;
import org.apache.ignite.session.SessionContext;
import org.apache.ignite.session.SessionContextProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** */
@ParameterizedClass(name = "mode={0}, isClnNode={1}")
@MethodSource("parameters")
public class SessionContextSqlFunctionTest extends GridCommonAbstractWrapperTest {
    /** */
    private static final String SESSION_ID = "sessionId";

    /** */
    private Ignite ign;

    /** */
    @Parameter(0)
    public CacheAtomicityMode mode;

    /** */
    @Parameter(1)
    public boolean isClnNode;

    /** */
    @MethodSource("parameters")
    private static List<Arguments> parameters() {
        List<Arguments> params = new ArrayList<>();

        for (CacheAtomicityMode m: CacheAtomicityMode.values()) {
            params.add(Arguments.of(m, false));
            params.add(Arguments.of(m, true));
        }

        return params;
    }

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSqlConfiguration(new SqlConfiguration()
            .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setSqlSchema("PUBLIC")
                .setSqlFunctionClasses(SessionContextSqlFunctions.class));

        return cfg;
    }

    /** */
    @BeforeEach
    void setup() throws Exception {
        ign = startGrids(3);

        if (isClnNode)
            ign = startClientGrid(3);

        query(ign, "create table PUBLIC.MYTABLE(id int primary key, sessionId varchar);");
    }

    /** {@inheritDoc} */
    @AfterEach
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testWhereClause() {
        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            query(ign, "insert into PUBLIC.MYTABLE(id, sessionId) values (?, ?);", i, sesId);
        }

        for (String sesId: F.asList("1", "2")) {
            Ignite ignApp = ign.withApplicationAttributes(F.asMap(SESSION_ID, sesId));

            List<List<?>> rows = query(ignApp, "select * from PUBLIC.MYTABLE where sessionId = sessionId();");

            assertEquals(50, rows.size());

            for (List<?> row: rows) {
                String actSesId = row.get(1).toString();

                assertEquals(sesId, actSesId);
            }
        }
    }

    /** */
    @Test
    public void testInsertClause() {
        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            Ignite ignApp = ign.withApplicationAttributes(F.asMap(SESSION_ID, sesId));

            query(ignApp, "insert into PUBLIC.MYTABLE(id, sessionId) values (" + i + ", sessionId());");
        }

        List<List<?>> res = query(ign, "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(50, res.size());

        res = query(ign, "select * from PUBLIC.MYTABLE where sessionId = 2");

        assertEquals(50, res.size());
    }

    /** */
    @Test
    public void testNestedQuery() {
        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            query(ign, "insert into PUBLIC.MYTABLE(id, sessionId) values (?, ?);", i, sesId);
        }

        String sesId = "1";

        Ignite ignApp = ign.withApplicationAttributes(F.asMap(SESSION_ID, sesId));

        List<List<?>> rows = query(ignApp, "select * from PUBLIC.MYTABLE where sessionId = (select sessionId());");

        int size = 0;

        for (List<?> row: rows) {
            String actSesId = row.get(1).toString();

            assertEquals(sesId, actSesId);

            size++;
        }

        assertEquals(50, size);
    }

    /** */
    @Test
    public void testOverwriteApplicationAttributes() {
        Ignite ignApp = ign;

        for (int i = 0; i < 100; i++) {
            String sesId = i % 2 == 0 ? "1" : "2";

            ignApp = ignApp.withApplicationAttributes(F.asMap(SESSION_ID, sesId));

            query(ignApp, "insert into PUBLIC.MYTABLE(id, sessionId) values (" + i + ", sessionId());");
        }

        List<List<?>> res = query(ign, "select * from PUBLIC.MYTABLE where sessionId = 1");

        assertEquals(50, res.size());

        res = query(ign, "select * from PUBLIC.MYTABLE where sessionId = 2");

        assertEquals(50, res.size());
    }

    /** */
    @Test
    public void testMultithreadApplication() throws Exception {
        String sesId = "1";

        Ignite ignApp = ign.withApplicationAttributes(F.asMap(SESSION_ID, sesId));

        IgniteInternalFuture<?> insertFut = multithreadedAsync(() -> {
            for (int i = 0; i < 100; i++)
                query(ignApp, "insert into PUBLIC.MYTABLE(id, sessionId) values (" + i + ", sessionId());");
        }, 1, "insert");

        insertFut.get(getTestTimeout());

        List<List<?>> res = query(ign, "select * from PUBLIC.MYTABLE where sessionId = " + sesId);

        assertEquals(100, res.size());
    }

    /** */
    private List<List<?>> query(Ignite ign, String sql, Object... args) {
        return ign.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery(sql).setArgs(args)).getAll();
    }

    /** */
    public static class SessionContextSqlFunctions {
        /** */
        @SessionContextProviderResource
        public SessionContextProvider sesCtxProv;

        /** */
        @QuerySqlFunction
        public String sessionId() {
            SessionContext sesCtx = sesCtxProv.getSessionContext();

            return sesCtx == null ? null : sesCtx.getAttribute(SESSION_ID);
        }
    }
}
