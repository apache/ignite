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

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.apache.presto.flex.FlexClient;
import org.apache.presto.flex.FlexConfig;
import org.testng.annotations.Test;

public class TestFlexClient
{
    @Test
    public void testMetadata()
    {
        FlexClient client = new FlexClient(new FlexConfig());
        assertEquals(client.getSchemaNames(), Arrays.asList("csv", "tsv", "txt", "raw", "excel"));
    }
}
