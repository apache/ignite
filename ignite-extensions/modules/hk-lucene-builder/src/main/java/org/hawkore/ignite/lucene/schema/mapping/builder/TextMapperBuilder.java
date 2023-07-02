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

import org.hawkore.ignite.lucene.schema.mapping.TextMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * {@link SingleColumnMapperBuilder} to build a new {@link TextMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class TextMapperBuilder extends SingleColumnMapperBuilder<TextMapper, TextMapperBuilder> {

    @JsonProperty("analyzer")
    private String analyzer;

    /**
     * Sets the name of the {@link org.apache.lucene.analysis.Analyzer} to be used.
     *
     * @param analyzer the name of the {@link org.apache.lucene.analysis.Analyzer} to be used
     * @return this
     */
    public TextMapperBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /**
     * Returns the {@link TextMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link TextMapper} represented by this
     */
    @Override
    public TextMapper build(String field) {
        return new TextMapper(field, column, validated, analyzer);
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
        TextMapperBuilder other = (TextMapperBuilder) obj;
        if (analyzer == null) {
            if (other.analyzer != null)
                return false;
        } else if (!analyzer.equals(other.analyzer))
            return false;
        return true;
    }
}
