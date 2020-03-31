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
package org.ebyhr.presto.flex.operator;

import com.facebook.presto.spi.SchemaNotFoundException;

public class PluginFactory
{
    public static FilePlugin create(String typeName)
    {
        switch (typeName.toLowerCase()) {
            case "csv":
                return new CsvPlugin();
            case "tsv":
                return new TsvPlugin();
            case "txt":
                return new TextPlugin();
            case "raw":
                return new RawPlugin();
            case "excel":
                return new ExcelPlugin();
            default:
                throw new SchemaNotFoundException(typeName);
        }
    }
}
