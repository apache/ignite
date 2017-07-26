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

package org.apache.ignite.internal.processors.query.h2;

import java.util.HashMap;
import java.util.Map;

/** */
public class H2ConnectionCache {
    /** */
    private H2ConnectionWrapper curr;

    /** */
    private final Map<String, H2ConnectionWrapper> schemaConns = new HashMap<>();

    /** */
    public H2ConnectionWrapper current()  {
        return curr;
    }

    /** */
    public void current(H2ConnectionWrapper conn) {
        curr = conn;
    }

    /** */
    public H2ConnectionWrapper get(String schema) {
        H2ConnectionWrapper conn = schemaConns.get(schema);

        curr = conn;

        return conn;
    }

    /** */
    public void put(String schema, H2ConnectionWrapper conn) {
        schemaConns.put(schema, conn);

        curr = conn;
    }

    /** */
    public void remove(String schema) {
        schemaConns.remove(schema);
    }
}
