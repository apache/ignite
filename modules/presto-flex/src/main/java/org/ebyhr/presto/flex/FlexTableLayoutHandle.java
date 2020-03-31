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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import java.util.Objects;

public class FlexTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final FlexTableHandle table;

    @JsonCreator
    public FlexTableLayoutHandle(@JsonProperty("table") FlexTableHandle table)
    {
        this.table = table;
    }

    @JsonProperty
    public FlexTableHandle getTable()
    {
        return table;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlexTableLayoutHandle that = (FlexTableLayoutHandle) o;
        return Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table);
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
