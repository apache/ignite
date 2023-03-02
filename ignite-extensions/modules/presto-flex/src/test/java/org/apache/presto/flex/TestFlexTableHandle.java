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

import org.apache.presto.flex.FlexTableHandle;
import org.testng.annotations.Test;

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;

public class TestFlexTableHandle
{
    private final FlexTableHandle tableHandle = new FlexTableHandle("connectorId", "schemaName", "tableName");

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<FlexTableHandle> codec = jsonCodec(FlexTableHandle.class);
        String json = codec.toJson(tableHandle);
        FlexTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new FlexTableHandle("connector", "schema", "table"), new FlexTableHandle("connector", "schema", "table"))
                .addEquivalentGroup(new FlexTableHandle("connectorX", "schema", "table"), new FlexTableHandle("connectorX", "schema", "table"))
                .addEquivalentGroup(new FlexTableHandle("connector", "schemaX", "table"), new FlexTableHandle("connector", "schemaX", "table"))
                .addEquivalentGroup(new FlexTableHandle("connector", "schema", "tableX"), new FlexTableHandle("connector", "schema", "tableX"))
                .check();
    }
}
