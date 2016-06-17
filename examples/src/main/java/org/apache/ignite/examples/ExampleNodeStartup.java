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

package org.apache.ignite.examples;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Starts up an empty node with example compute configuration.
 */
public class ExampleNodeStartup {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignite start = Ignition.start("examples/config/example-ignite.xml");
        CacheConfiguration<Integer, BinaryObject> cfg = new CacheConfiguration<>();
        cfg.setQueryEntities(new ArrayList<QueryEntity>() {{
            QueryEntity e = new QueryEntity();
            e.setKeyType("java.lang.Integer");
            e.setValueType("BinaryTest");
            e.setFields(new LinkedHashMap<String, String>(){{
                put("name", "java.lang.String");
            }});
            add(e);
        }});
        IgniteCache<Integer, BinaryObject> cache = start.getOrCreateCache(cfg).withKeepBinary();
        BinaryObjectBuilder builder = start.binary().builder("BinaryTest");
        builder.setField("name", "Test");
        cache.put(1, builder.build());

        QueryCursor<List<?>> query = cache.query(new SqlFieldsQuery("select name from BinaryTest"));
        System.out.println(query.getAll());
    }
}