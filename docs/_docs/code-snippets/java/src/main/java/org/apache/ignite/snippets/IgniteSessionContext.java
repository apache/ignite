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
package org.apache.ignite.snippets;

import javax.cache.Cache;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.resources.SessionContextProviderResource;
import org.apache.ignite.session.SessionContextProvider;
import org.jetbrains.annotations.Nullable;

public class IgniteSessionContext {

    //tag::context[]
    /** SessionContext interface. It's an entrypoint for all attributes. */
    public interface SessionContext {

        /** @return Attribute by name. */
        public @Nullable String getAttribute(String attrName);
    }
    //end::context[]

    //tag::sql-function[]
    public class MyFunctions {

        @SessionContextProviderResource
        public SessionContextProvider sesCtxProv;

        @QuerySqlFunction
        public String sessionId() {
            return sesCtxProv.getSessionContext().getAttribute("SESSION_ID");
        }
    }
    //end::sql-function[]

    //tag::cache-interceptor[]
    public class SessionContextCacheInterceptor implements CacheInterceptor<Integer, String> {
        /** */
        @SessionContextProviderResource
        private SessionContextProvider sesCtxPrv;

        /** */
        @Override public @Nullable String onGet(Integer key, @Nullable String val) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onGet");

            return ret == null ? val : ret + key;
        }

        /** */
        @Override public @Nullable String onBeforePut(Cache.Entry<Integer, String> entry, String newVal) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onBeforePut");

            return ret == null ? newVal : ret + entry.getKey();
        }

        /** */
        @Override public @Nullable IgniteBiTuple<Boolean, String> onBeforeRemove(Cache.Entry<Integer, String> entry) {
            String ret = sesCtxPrv.getSessionContext().getAttribute("onBeforeRemove");

            return new IgniteBiTuple<>(ret != null, entry.getValue());
        }
    }
    //end::cache-interceptor[]

    public void igniteSessContext() {
        IgniteConfiguration ignCfg = new IgniteConfiguration();
        //tag::ignite-context[]
        try (Ignite ign = Ignition.start(ignCfg)) {

            Map<String, String> appAttrs = F.asMap("SESSION_ID", "1234");

            Ignite app = Ignite.withApplicationAttributes(appAttrs);

            //Your code here...
        }
        //end::ignite-context[]
    }

    public void jdbcSessContext() {
        String URL = "test";
        //tag::jdbc-context[]
        // JDBC connection to Ignite server.
        try (Connection conn = DriverManager.getConnection(URL)) {

            conn.setClientInfo("SESSION_ID", "1234");

            //Your code here...
        }
        //end::jdbc-context[]
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}