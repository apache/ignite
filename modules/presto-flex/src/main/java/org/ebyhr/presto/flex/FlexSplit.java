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
import com.google.common.collect.ImmutableList;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static java.util.Objects.requireNonNull;

public class FlexSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schemaName;
    private final String tableName;
    private final boolean remotelyAccessible;

    @JsonCreator
    public FlexSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schema name is null");
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");

        remotelyAccessible = true;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    
    public boolean isRemotelyAccessible()
    {
        // only http or https is remotely accessible
        return remotelyAccessible;
    }

   
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

	@Override
	public NodeSelectionStrategy getNodeSelectionStrategy() {
		// TODO Auto-generated method stub
		 return NO_PREFERENCE;
	}

	@Override
	public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates) {
		// TODO Auto-generated method stub
		return ImmutableList.of();
	}
}
