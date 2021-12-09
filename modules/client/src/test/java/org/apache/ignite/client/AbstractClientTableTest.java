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

import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ALL_COLUMNS;
import static org.apache.ignite.client.fakes.FakeIgniteTables.TABLE_ONE_COLUMN;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.UUID;
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

    protected static final LocalDate localDate = LocalDate.now();

    protected static final LocalTime localTime = LocalTime.now();

    protected static final Instant instant = Instant.now();

    protected static final UUID uuid = UUID.randomUUID();

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
        server.tables().createTableIfNotExists(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }

    protected static Tuple allClumnsTableKey(long id) {
        return Tuple.create().set("gid", id).set("id", String.valueOf(id));
    }

    protected static Tuple allColumnsTableVal(String name) {
        return Tuple.create()
                .set("gid", DEFAULT_ID)
                .set("id", String.valueOf(DEFAULT_ID))
                .set("zbyte", (byte) 11)
                .set("zshort", (short) 12)
                .set("zint", (int) 13)
                .set("zlong", (long) 14)
                .set("zfloat", (float) 1.5)
                .set("zdouble", (double) 1.6)
                .set("zdate", localDate)
                .set("ztime", localTime)
                .set("ztimestamp", instant)
                .set("zstring", name)
                .set("zbytes", new byte[]{1, 2})
                .set("zbitmask", BitSet.valueOf(new byte[]{32}))
                .set("zdecimal", BigDecimal.valueOf(21))
                .set("znumber", BigInteger.valueOf(22))
                .set("zuuid", uuid);
    }

    protected Table fullTable() {
        server.tables().createTableIfNotExists(TABLE_ALL_COLUMNS, tbl -> tbl.changeReplicas(1));

        return client.tables().table(TABLE_ALL_COLUMNS);
    }

    protected static Tuple oneColumnTableKey(String id) {
        return Tuple.create().set("id", id);
    }

    protected Table oneColumnTable() {
        server.tables().createTableIfNotExists(TABLE_ONE_COLUMN, tbl -> tbl.changeReplicas(1));

        return client.tables().table(TABLE_ONE_COLUMN);
    }
}
