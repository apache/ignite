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
package io.prestosql.plugin.ignite;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;

import static io.prestosql.spi.session.PropertyMetadata.enumProperty;

public final class IgniteSessionProperties
        implements SessionPropertiesProvider
{
    private static final String ARRAY_MAPPING = "array_mapping";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public IgniteSessionProperties(IgniteConfig postgreSqlConfig)
    {
        sessionProperties = ImmutableList.of(
        		PropertyMetadata.stringProperty("template", "setting template of table", null, true)
        		
        		);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

   
    

    /** 
     * add@byron support ignite with property
     * @return the table properties for this connector
     */
    public List<PropertyMetadata<?>> getTableProperties()
    {
    	return Arrays.asList(
    			PropertyMetadata.stringProperty(IgniteClient.PRIMARY_KEY, "setting primay key column of table", null, false),
    			PropertyMetadata.stringProperty("affinity_key", "setting affinity_key column of table", null, false),
    			PropertyMetadata.stringProperty("template", "setting template of table", null, true),
    			PropertyMetadata.stringProperty("cache_name", "setting cache name of table", null, true)
    			
    			);
    }

    /**add@byron
     * @return the column properties for this connector
     */
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return Arrays.asList(PropertyMetadata.stringProperty(IgniteClient.PRIMARY_KEY, "setting column is primay key", null, false));
    }
}
