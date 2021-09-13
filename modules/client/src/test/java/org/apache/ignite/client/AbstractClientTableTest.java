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

package org.apache.ignite.client;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;

/**
 * Base class for client table tests.
 */
public class AbstractClientTableTest extends AbstractClientTest {
    /** Default name. */
    protected static final String DEFAULT_NAME = "John";

    /** Default id. */
    protected static final Long DEFAULT_ID = 123L;

    protected static Tuple[] sortedTuples(Collection<Tuple> tuples) {
        Tuple[] res = tuples.toArray(new Tuple[0]);

        Arrays.sort(res, (x, y) -> (int) (x.longValue(0) - y.longValue(0)));

        return res;
    }

    protected static Tuple tuple() {
        return Tuple.create()
                .set("id", DEFAULT_ID)
                .set("name", DEFAULT_NAME);
    }

    protected static Tuple tuple(Long id) {
        return Tuple.create()
                .set("id", id);
    }

    protected static Tuple tuple(Long id, String name) {
        return Tuple.create()
                .set("id", id)
                .set("name", name);
    }

    protected static Tuple defaultTupleKey() {
        return Tuple.create().set("id", DEFAULT_ID);
    }

    protected static Tuple tupleKey(long id) {
        return Tuple.create().set("id", id);
    }

    protected static Tuple tupleVal(String name) {
        return Tuple.create().set("name", name);
    }

    protected Table defaultTable() {
        server.tables().getOrCreateTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }
}
