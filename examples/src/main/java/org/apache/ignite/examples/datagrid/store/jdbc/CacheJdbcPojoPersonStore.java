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

package org.apache.ignite.examples.datagrid.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.h2.tools.*;
import org.jetbrains.annotations.*;

import javax.cache.integration.*;
import java.io.*;

/**
 * TODO: Add class description.
 */
public class CacheJdbcPojoPersonStore<K, V> extends CacheJdbcPojoStore {
    public CacheJdbcPojoPersonStore() throws IgniteException {
        dataSrc = org.h2.jdbcx.JdbcConnectionPool.create("jdbc:h2:mem:ExampleDb;DB_CLOSE_DELAY=-1", "sa", "");

        File script = U.resolveIgnitePath("examples/config/store/initdb.script");

        if (script == null)
            throw new IgniteException("Failed to find initial database script: " + "examples/config/store/initdb.script");

        try {
            RunScript.execute(dataSrc.getConnection(), new FileReader(script));
        }
        catch (Exception e) {
            throw new IgniteException("Failed to initialize database", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args)
        throws CacheLoaderException {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        super.loadCache(clo, "java.lang.Long", "select * from PERSON limit " + entryCnt);
    }
}
