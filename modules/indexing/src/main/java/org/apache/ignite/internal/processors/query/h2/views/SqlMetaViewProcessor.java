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

package org.apache.ignite.internal.processors.query.h2.views;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;

/**
 * Meta views processor.
 */
public class SqlMetaViewProcessor {
    /** Table schema name. */
    public static final String SCHEMA_NAME = "IGNITE";

    /** Registered views. */
    private final Map<String, SqlMetaView> registeredViews = new HashMap<>();

    /**
     * Starts meta views processor.
     *
     * @param ctx Kernal context.
     * @param idx indexing.
     */
    public void start(GridKernalContext ctx, IgniteH2Indexing idx) {
        IgniteLogger log = ctx.log(SqlMetaViewProcessor.class);

        log.info("Starting meta views processor");

        Connection c = idx.connectionForSchema(SCHEMA_NAME);

        try {
            registeredViews.clear();

            Collection<SqlMetaView> viewsToRegister = new ArrayList<>();

            viewsToRegister.add(new SqlMetaViewNodes(ctx));

            for (SqlMetaView view : viewsToRegister) {
                SqlMetaTableEngine.registerView(c, view);

                registeredViews.put(view.getTableName(), view);

                if (log.isDebugEnabled())
                    log.debug("Registered meta view: " + view.getTableName());
            }
        }
        catch (SQLException e) {
            log.error("Failed to register meta views: ", e);

            throw new IgniteException(e);
        }
    }

    /**
     * Gets registered views.
     */
    public Map<String, SqlMetaView> getRegisteredViews() {
        return Collections.unmodifiableMap(registeredViews);
    }
}
