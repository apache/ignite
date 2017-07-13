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

import java.util.Map;
import java.util.WeakHashMap;

/** */
public class H2ConnectionCache {
    /** */
    private H2ConnectionWrapper current;

    /** */
    private final Map<H2Schema, H2ConnectionWrapper> schemaCons = new WeakHashMap<H2Schema, H2ConnectionWrapper>();

    /** */
    public H2ConnectionWrapper current()  {
        return current;
    }

    /** */
    public void current(H2ConnectionWrapper conn) {
        current = conn;
    }

    /** */
    public H2ConnectionWrapper get(H2Schema schema) {
        H2ConnectionWrapper conn = schemaCons.get(schema);

        current = conn;

        return conn;
    }

    /** */
    public void put(H2Schema schema, H2ConnectionWrapper conn) {
        schemaCons.put(schema, conn);

        current = conn;
    }

    /** */
    public void remove(H2Schema schema) {
        schemaCons.remove(schema);
    }
}
