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
package org.ebyhr.presto.flex;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestFlexRecordSetProvider
{
    private URI CSV_URI = URI.create("http://s3.amazonaws.com/presto-example/v2/numbers-1.csv");

    @Test
    public void testGetRecordSet()
    {
        FlexRecordSetProvider recordSetProvider = new FlexRecordSetProvider(new FlexConnectorId("test"));
        RecordSet recordSet = recordSetProvider.getRecordSet(FlexTransactionHandle.INSTANCE, SESSION, new FlexSplit("test", "csv", CSV_URI.toString()), ImmutableList.of(
                new FlexColumnHandle("test", "text", createUnboundedVarcharType(), 0),
                new FlexColumnHandle("test", "value", createUnboundedVarcharType(), 1)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, String> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getSlice(1).toStringUtf8());
        }
        assertEquals(data, ImmutableMap.<String, String> builder()
                .put("two", "2")
                .put("three", "3")
                .build());
    }
}
