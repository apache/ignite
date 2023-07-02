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
package org.hawkore.ignite.lucene.builder.index.schema.analysis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link Analyzer} for tartarus.org snowball {@code Analyzer}.
 *
 * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
 * Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SnowballAnalyzer extends Analyzer {

    /**
     * The language. The supported values are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch,
     * Swedish, Norwegian, Danish, Russian, Finnish, Hungarian and Turkish.
     */
    @JsonProperty("language")
    final String language;

    /** The comma-separated stopwords list. */
    @JsonProperty("stopwords")
    String stopwords;

    /**
     * Builds a new {@link SnowballAnalyzer} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     */
    @JsonCreator
    public SnowballAnalyzer(@JsonProperty("language") String language) {
        this.language = language;
    }

    /**
     * Returns this with the specified stopwords.
     *
     * @param stopwords the comma-separated stopwords list
     * @return this with the specified stopwords
     */
    public SnowballAnalyzer stopwords(String stopwords) {
        this.stopwords = stopwords;
        return this;
    }
}
