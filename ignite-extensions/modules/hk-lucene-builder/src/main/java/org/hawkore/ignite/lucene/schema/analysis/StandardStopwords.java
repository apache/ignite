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

import java.util.Locale;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;

/**
 * Prebuilt Lucene analyzer stopwords that can be instantiated by language name.
 */
public enum StandardStopwords {
    ENGLISH() {
        @Override
        protected CharArraySet build() {
            return EnglishAnalyzer.getDefaultStopSet();
        }
    },
    FRENCH {
        @Override
        protected CharArraySet build() {
            return FrenchAnalyzer.getDefaultStopSet();
        }
    },
    SPANISH {
        @Override
        protected CharArraySet build() {
            return SpanishAnalyzer.getDefaultStopSet();
        }
    },
    PORTUGUESE {
        @Override
        protected CharArraySet build() {
            return PortugueseAnalyzer.getDefaultStopSet();
        }
    },
    ITALIAN {
        @Override
        protected CharArraySet build() {
            return ItalianAnalyzer.getDefaultStopSet();
        }
    },
    GERMAN {
        @Override
        protected CharArraySet build() {
            return GermanAnalyzer.getDefaultStopSet();
        }
    },
    DUTCH {
        @Override
        protected CharArraySet build() {
            return DutchAnalyzer.getDefaultStopSet();
        }
    },
    SWEDISH {
        @Override
        protected CharArraySet build() {
            return SwedishAnalyzer.getDefaultStopSet();
        }
    },
    NORWEGIAN {
        @Override
        protected CharArraySet build() {
            return NorwegianAnalyzer.getDefaultStopSet();
        }
    },
    DANISH {
        @Override
        protected CharArraySet build() {
            return DanishAnalyzer.getDefaultStopSet();
        }
    },
    RUSSIAN {
        @Override
        protected CharArraySet build() {
            return RussianAnalyzer.getDefaultStopSet();
        }
    },
    FINNISH {
        @Override
        protected CharArraySet build() {
            return FinnishAnalyzer.getDefaultStopSet();
        }
    },
    IRISH {
        @Override
        protected CharArraySet build() {
            return IrishAnalyzer.getDefaultStopSet();
        }
    },
    HUNGARIAN {
        @Override
        protected CharArraySet build() {
            return HungarianAnalyzer.getDefaultStopSet();
        }
    },
    TURKISH {
        @Override
        protected CharArraySet build() {
            return TurkishAnalyzer.getDefaultStopSet();
        }
    },
    ARMENIAN {
        @Override
        protected CharArraySet build() {
            return ArmenianAnalyzer.getDefaultStopSet();
        }
    },
    BASQUE {
        @Override
        protected CharArraySet build() {
            return BasqueAnalyzer.getDefaultStopSet();
        }
    },
    CATALAN {
        @Override
        protected CharArraySet build() {
            return CatalanAnalyzer.getDefaultStopSet();
        }
    };

    /**
     * Returns a new instance of the defined analyzer.
     *
     * @return A new instance of the defined analyzer.
     */
    abstract CharArraySet build();

    /**
     * Returns the prebuilt analyzer stopwords list identified by the specified name, or {@code null} if there is no
     * such stopwords list.
     *
     * @param name
     *     the name of the searched analyzer
     * @return the prebuilt analyzer stopwords list identified by the specified name, or {@code null} if there is no
     *     such stopwords list
     */
    public static CharArraySet get(String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT)).get();
        } catch (IllegalArgumentException e) {
            return CharArraySet.EMPTY_SET;
        }
    }

    /**
     * Returns the analyzer stopwords defined by this.
     *
     * @return the analyzer stopwords list
     */
    public CharArraySet get() {
        CharArraySet stopWords = StandardAnalyzers.DefaultStopSetHolder.CUSTOM_LANG_STOP_SET
                                     .get(this.name().toLowerCase(Locale.ROOT));
        if (stopWords != null) {
            // use custom stop words
            return stopWords;
        }
        // use prebuilt default stop words
        return build();
    }
}
