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

package org.apache.ignite.internal.systemview;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.spi.systemview.view.SqlQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_QRY_VIEW;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;

/**
 * System view security test.
 */
public class SystemViewSecurityTest extends AbstractSecurityTest {
    /** {@inheritDoc} */
    @Override protected TestSecurityData[] securityData() {
        return new TestSecurityData[] {new TestSecurityData("thin-client", ALL_PERMISSIONS)};
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlQueryView() throws Exception {
        IgniteEx srv = startGridAllowAll("srv");
        IgniteEx client = startClientAllowAll("client");

        ClientConfiguration cfg = new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setUserName("thin-client")
            .setUserPassword("");

        try (IgniteClient thinClient = Ignition.startClient(cfg)) {
            SqlFieldsQuery srvSql = new SqlFieldsQuery("SELECT * FROM (VALUES (1))");
            SqlFieldsQuery clientSql = new SqlFieldsQuery("SELECT * FROM (VALUES (1),(2))");
            SqlFieldsQuery thinClientSql = new SqlFieldsQuery("SELECT * FROM (VALUES (1),(2),(3))").setPageSize(1);

            srv.context().query().querySqlFields(srvSql, false).iterator().hasNext();
            client.compute().run(() -> client.context().query().querySqlFields(clientSql, false).iterator().hasNext());
            thinClient.query(thinClientSql).iterator().hasNext();

            Map<String, Object> expLogins = new HashMap<>();

            expLogins.put(srvSql.getSql(), srv.context().igniteInstanceName());
            expLogins.put(clientSql.getSql(), client.context().igniteInstanceName());
            expLogins.put(thinClientSql.getSql(), cfg.getUserName());

            SystemView<SqlQueryView> views = srv.context().systemView().view(SQL_QRY_VIEW);

            assertEquals(expLogins.size(), views.size());

            for (SqlQueryView view : views) {
                Object login = srv.context().security().authenticatedSubject(view.subjectId()).login();

                assertTrue(expLogins.remove(view.sql(), login));
            }

            assertTrue(expLogins.isEmpty());
        }
    }
}
