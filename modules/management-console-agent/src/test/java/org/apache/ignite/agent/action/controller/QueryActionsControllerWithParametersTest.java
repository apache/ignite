/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.action.controller;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import org.apache.ignite.IgniteException;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.query.QueryArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;

/**
 * Query actions controller with parameters test.
 */
public class QueryActionsControllerWithParametersTest extends AbstractActionControllerTest {
    /** {@inheritDoc} */
    @Before
    @Override public void startup() throws Exception {
        super.startup();

        String insertAllTypesQry_1 = getInsertAllTypesQuery(
            1,
            false,
            3,
            10,
            50,
            new BigDecimal(900),
            2d,
            35f,
            "12:34:57",
            "2019-05-05",
            "2019-05-05 12:34:57",
            "char_1",
            'a',
            UUID.fromString("a-a-a-a-a")
        );

        String insertAllTypesQry_2 = getInsertAllTypesQuery(
            2,
            true,
            30,
            100,
            500,
            new BigDecimal(9000),
            20d,
            350f,
            "13:40:25",
            "2019-11-15",
            "2019-11-15 13:40:25",
            "char_2",
            'b',
            UUID.fromString("b-b-b-b-b")
        );

        Request initReq = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText(getAllTypeCreateQueryTable() + insertAllTypesQry_1 + insertAllTypesQry_2)
                    .setPageSize(10)
            );

        executeAction(initReq, (r) -> r.getStatus() == COMPLETED);
    }

    /**
     * Should execute query with IN parameter.
     */
    @Test
    public void shouldExecuteQueryWithInParameter() {
        Request req = new Request()
            .setAction("QueryActions.executeSqlQuery")
            .setId(UUID.randomUUID())
            .setArgument(
                new QueryArgument()
                    .setQueryId("qry")
                    .setQueryText("SELECT * FROM mc_agent_all_types_table WHERE id IN (?, ?)")
                    .setPageSize(10)
                    .setParameters(new Object[]{"1", "2"})
            );

        executeAction(req, (r) -> {
            if (r.getStatus() == COMPLETED) {
                DocumentContext ctx = parse(r.getResult());

                JSONArray rows = ctx.read("$[0].rows[*]");

                return rows.size() == 2;
            }

            return false;
        });
    }

    /**
     * Should execute query with simple single parameters.
     */
    @Test
    public void shouldExecuteQueryWithSimpleSingleParameters() {
        Stream.of(
            tupleOf("ID", "1"),
            tupleOf("VALUE_BOOL", "true"),
            tupleOf("VALUE_TINY_INT", "3"),
            tupleOf("VALUE_SMALL_INT", "100"),
            tupleOf("VALUE_BIG_INT", "50"),
            tupleOf("VALUE_DECIMAL", "9000"),
            tupleOf("VALUE_DOUBLE", "2.0"),
            tupleOf("VALUE_REAL", "350.0"),
            tupleOf("VALUE_TIME", "12:34:57"),
            tupleOf("VALUE_DATE", "2019-11-15"),
            tupleOf("VALUE_TIMESTAMP", "2019-05-05 12:34:57.0"),
            tupleOf("VALUE_VARCHAR", "char_2"),
            tupleOf("VALUE_CHAR", "b"),
            tupleOf("VALUE_UUID", UUID.fromString("a-a-a-a-a").toString())
        ).forEach(t -> {
            String fieldName = t.getKey();

            Request req = new Request()
                .setAction("QueryActions.executeSqlQuery")
                .setId(UUID.randomUUID())
                .setArgument(
                    new QueryArgument()
                        .setQueryId("qry")
                        .setQueryText(format("SELECT * FROM mc_agent_all_types_table WHERE %s = ?", fieldName))
                        .setPageSize(10)
                        .setParameters(new Object[]{t.getValue()})
                );

            executeAction(req, (r) -> {
                if (r.getStatus() == COMPLETED) {
                    DocumentContext ctx = parse(r.getResult());

                    JSONArray rows = ctx.read("$[0].rows[*]");

                    JSONArray row = ctx.read("$[0].rows[0][*]");

                    JSONArray cols = ctx.read("$[0].columns[*]");

                    int fieldIdx = findFieldIndex(fieldName, cols);

                    Object expVal = t.getValue();

                    String actVal = row.get(fieldIdx).toString();

                    if ("VALUE_DATE".equals(fieldName))
                        actVal = new Date(Long.parseLong(actVal)).toString();
                    else if ("VALUE_TIMESTAMP".equals(fieldName))
                        actVal = new Timestamp(Long.parseLong(actVal)).toString();

                    if (rows.size() == 1 && actVal.equals(expVal))
                        return true;
                    else
                        throw new RuntimeException(
                            format("Received bad result [field=%s, actVal=%s, expVal=%s]", fieldName, actVal, expVal)
                        );
                }

                return false;
            });
        });
    }

    /**
     * @param fieldName Field name.
     * @param cols Columns.
     */
    private int findFieldIndex(String fieldName, JSONArray cols) {
        int idx;

        for (idx = 0; idx < cols.size(); idx++) {
            String fn = ((Map<String, String>)cols.get(idx)).get("fieldName");

            if (fieldName.equals(fn))
                break;
        }

        return idx;
    }

    /**
     * @param field Field.
     * @param val   Value.
     */
    private IgniteBiTuple<String, Object> tupleOf(String field, Object val) {
        return new IgniteBiTuple<>(field, val);
    }

    /**
     * @return Create table query string with all types.
     */
    private String getAllTypeCreateQueryTable() {
        return "CREATE TABLE mc_agent_all_types_table (" +
            " id INT, " +
            " value_bool BOOLEAN," +
            " value_tiny_int TINYINT," +
            " value_small_int SMALLINT," +
            " value_big_int BIGINT," +
            " value_decimal DECIMAL," +
            " value_double DOUBLE," +
            " value_real REAL," +
            " value_time TIME," +
            " value_date DATE," +
            " value_timestamp TIMESTAMP," +
            " value_varchar VARCHAR," +
            " value_char CHAR," +
            " value_uuid UUID," +
            " PRIMARY KEY (id)" +
            ");";
    }

    /**
     * @param id          Id.
     * @param boolVal     Bool value.
     * @param tinyIntVal  Tiny int value.
     * @param smallIntVal Small int value.
     * @param bigIntVal   Big int value.
     * @param decimalVal  Decimal value.
     * @param doubleVal   Double value.
     * @param realVal     Real value.
     * @param timeVal     Time value.
     * @param dateVal     Date value.
     * @param tsVal       Timestamp value.
     * @param varcharVal  Varchar value.
     * @param cVal        Closure value.
     * @param uuidVal     Uuid value.
     */
    private String getInsertAllTypesQuery(
        int id,
        boolean boolVal,
        int tinyIntVal,
        int smallIntVal,
        int bigIntVal,
        BigDecimal decimalVal,
        double doubleVal,
        float realVal,
        String timeVal,
        String dateVal,
        String tsVal,
        String varcharVal,
        char cVal,
        UUID uuidVal
    ) {
        return "INSERT INTO mc_agent_all_types_table VALUES("
            + id + ", "
            + boolVal + ", "
            + tinyIntVal + ", "
            + smallIntVal + ", "
            + bigIntVal + ", "
            + decimalVal + ", "
            + doubleVal + ", "
            + realVal + ", "
            + format("\'%s\'", timeVal) + ", "
            + format("\'%s\'", dateVal) + ", "
            + format("\'%s\'", tsVal) + ", "
            + format("\'%s\'", varcharVal) + ", "
            + format("\'%s\'", cVal) + ", "
            + format("\'%s\'", uuidVal) +
            ");";
    }

    /**
     * @param obj Object.
     */
    private DocumentContext parse(Object obj) {
        try {
            return JsonPath.parse(mapper.writeValueAsString(obj));
        }
        catch (JsonProcessingException e) {
            throw new IgniteException(e);
        }
    }
}
