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

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

import org.apache.presto.flex.FlexSplit;
import org.testng.annotations.Test;

import io.airlift.json.JsonCodec;

public class TestFlexSplit
{
    private final FlexSplit split = new FlexSplit("connectorId", "schemaName", "tableName");

    @Test
    public void testAddresses()
    {
        // http split with default port
        FlexSplit httpSplit = new FlexSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpSplit = new FlexSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with default port
        FlexSplit httpsSplit = new FlexSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpsSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpsSplit = new FlexSplit("connectorId", "schemaName", "tableName");
        assertEquals(httpsSplit.isRemotelyAccessible(), true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<FlexSplit> codec = jsonCodec(FlexSplit.class);
        String json = codec.toJson(split);
        FlexSplit copy = codec.fromJson(json);
        assertEquals(copy.getConnectorId(), split.getConnectorId());
        assertEquals(copy.getSchemaName(), split.getSchemaName());
        assertEquals(copy.getTableName(), split.getTableName());

        assertEquals(copy.isRemotelyAccessible(), true);
    }
}
