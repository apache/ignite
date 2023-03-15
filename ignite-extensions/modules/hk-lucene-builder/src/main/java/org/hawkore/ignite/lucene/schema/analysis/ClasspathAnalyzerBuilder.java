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
package org.hawkore.ignite.lucene.schema.analysis;

import java.lang.reflect.Constructor;

import org.apache.lucene.analysis.Analyzer;
import org.hawkore.ignite.lucene.IndexException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link AnalyzerBuilder} for building {@link Analyzer}s in classpath using its default (no args) constructor.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class ClasspathAnalyzerBuilder extends AnalyzerBuilder {

    @JsonProperty("class")
    private final String className;

    /**
     * Builds a new {@link AnalyzerBuilder} using the specified {@link Analyzer} full class name.
     *
     * @param className an {@link Analyzer} full qualified class name
     */
    @JsonCreator
    public ClasspathAnalyzerBuilder(@JsonProperty("class") String className) {
        this.className = className;

    }

    /** {@inheritDoc} */
    @Override
    public Analyzer analyzer() {
        try {
            Class<?> analyzerClass = Class.forName(className);
            Constructor<?> constructor = analyzerClass.getConstructor();
            return (Analyzer) constructor.newInstance();
        } catch (Exception e) {
            throw new IndexException(e, "Not found analyzer '{}'", className);
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((className == null) ? 0 : className.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ClasspathAnalyzerBuilder other = (ClasspathAnalyzerBuilder) obj;
        if (className == null) {
            if (other.className != null)
                return false;
        } else if (!className.equals(other.className))
            return false;
        return true;
    }
    
}
