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
package org.hawkore.ignite.lucene.builder.index.schema.mapping;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Mapper} to map a string, tokenized field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapper extends SingleColumnMapper<TextMapper> {

    /** The analyzer. */
    @JsonProperty("analyzer")
    String analyzer;

    /**
     * Sets the name of the {@code Analyzer} to be used.
     *
     * @param analyzer the name of the {@code Analyzer}
     * @return this with the specified analyzer
     */
    public TextMapper analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((analyzer == null) ? 0 : analyzer.hashCode());
        return result;
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
        if (getClass() != obj.getClass())
            return false;
        TextMapper other = (TextMapper) obj;
        if (analyzer == null) {
            if (other.analyzer != null)
                return false;
        } else if (!analyzer.equals(other.analyzer))
            return false;
        return true;
    }
}
