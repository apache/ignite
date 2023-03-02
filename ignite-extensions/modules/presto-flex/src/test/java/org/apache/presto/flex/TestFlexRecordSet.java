/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.presto.flex;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.presto.flex.FlexColumnHandle;
import org.apache.presto.flex.FlexRecordSet;
import org.apache.presto.flex.FlexSplit;
import org.testng.annotations.Test;

import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;

public class TestFlexRecordSet
{
    private static final URI CSV = URI.create("https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers-2.csv");
    private static final URI TSV = URI.create("https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/numbers.tsv");
    private static final URI EXCEL = URI.create("https://raw.githubusercontent.com/ebyhr/presto-flex/master/src/test/resources/example-data/sample.xlsx");

    @Test
    public void testGetColumnTypes()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", BIGINT, 1)));
        assertEquals(recordSet.getColumnTypes(), List.of(createUnboundedVarcharType(), BIGINT));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(BIGINT, createUnboundedVarcharType()));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        assertEquals(recordSet.getColumnTypes(), List.of(BIGINT, BIGINT, createUnboundedVarcharType()));

        recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of());
        assertEquals(recordSet.getColumnTypes(), List.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", BIGINT, 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), BIGINT);

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, Map.of("eleven", 11L, "twelve", 12L));
    }

    @Test
    public void testTsvCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "tsv", TSV.toString()), List.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }
        assertEquals(data, Map.of("two", "2", "three", "3"));
    }

    @Test
    public void testTxtCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "txt", CSV.toString()), List.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());

        List<String> data = new LinkedList<>();
        while (cursor.advanceNextPosition()) {
            data.add(cursor.getSlice(0).toStringUtf8());
            assertFalse(cursor.isNull(0));
        }
        assertEquals(data, List.of("ten, 10", "eleven, 11", "twelve, 12"));
    }

    @Test
    public void testJsonCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "raw", CSV.toString()), List.of(
                new FlexColumnHandle("test", "data", createUnboundedVarcharType(), 0)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());

        List<String> data = new LinkedList<>();
        while (cursor.advanceNextPosition()) {
            data.add(cursor.getSlice(0).toStringUtf8());
            assertFalse(cursor.isNull(0));
        }
        assertEquals(data, List.of("ten, 10\neleven, 11\ntwelve, 12"));
    }

    @Test
    public void testExcelCursorSimple()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "excel", EXCEL.toString()), List.of(
                new FlexColumnHandle("test", "c1", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "c2", createUnboundedVarcharType(), 1)));
        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), createUnboundedVarcharType());
        assertEquals(cursor.getType(1), createUnboundedVarcharType());

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
            assertFalse(cursor.isNull(0));
            assertFalse(cursor.isNull(1));
        }

        assertEquals(data, Map.of("a", "1", "b", "2"));
    }

    @Test
    public void testCursorMixedOrder()
    {
        RecordSet recordSet = new FlexRecordSet(new FlexSplit("test", "csv", CSV.toString()), List.of(
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "value", BIGINT, 1),
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0)));
        RecordCursor cursor = recordSet.cursor();

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            assertEquals(cursor.getLong(0), cursor.getLong(1));
            data.put(cursor.getSlice(2).toStringUtf8(), cursor.getLong(0));
        }
        assertEquals(data, Map.of("eleven", 11L, "twelve", 12L));
    }
}
