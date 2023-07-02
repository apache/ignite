/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.schema.mapping.builder;

import org.hawkore.ignite.lucene.schema.mapping.InetMapper;
 
/**
 * {@link SingleColumnMapperBuilder} to build a new {@link InetMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class InetMapperBuilder extends SingleColumnMapperBuilder<InetMapper, InetMapperBuilder> {

    /**
     * Returns the {@link InetMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link InetMapper} represented by this
     */
    @Override
    public InetMapper build(String field) {
        return new InetMapper(field, column, validated);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        return prime * result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        return getClass() != obj.getClass();
    }
}
