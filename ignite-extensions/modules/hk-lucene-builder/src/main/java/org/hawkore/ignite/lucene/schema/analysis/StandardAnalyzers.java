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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.util.IOUtils;

/**
 * Prebuilt Lucene {@link Analyzer}s that can be instantiated by name.
 */
public enum StandardAnalyzers {
    STANDARD() {
        @Override
        protected Analyzer build() {
            return new StandardAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new StandardAnalyzer(stopWords);
        }
    },
    DEFAULT() {
        @Override
        protected Analyzer build() {
            return STANDARD.build();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return STANDARD.build(stopWords);
        }
    },
    KEYWORD() {
        @Override
        protected Analyzer build() {
            return new KeywordAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new KeywordAnalyzer();
        }
    },
    STOP {
        @Override
        protected Analyzer build() {
            try {
				return new StopAnalyzer(Path.of("conf/stopwords.txt"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new StopAnalyzer(stopWords);
        }
    },
    WHITESPACE {
        @Override
        protected Analyzer build() {
            return new WhitespaceAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new WhitespaceAnalyzer();
        }
    },
    SIMPLE {
        @Override
        protected Analyzer build() {
            return new SimpleAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new SimpleAnalyzer();
        }
    },
    CLASSIC {
        @Override
        protected Analyzer build() {
            return new ClassicAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new ClassicAnalyzer(stopWords);
        }
    },
    ARABIC {
        @Override
        protected Analyzer build() {
            return new ArabicAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new ArabicAnalyzer(stopWords);
        }
    },
    ARMENIAN {
        @Override
        protected Analyzer build() {
            return new ArmenianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new ArmenianAnalyzer(stopWords);
        }
    },
    BASQUE {
        @Override
        protected Analyzer build() {
            return new BasqueAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new BasqueAnalyzer(stopWords);
        }
    },
    BRAZILIAN {
        @Override
        protected Analyzer build() {
            return new BrazilianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new BrazilianAnalyzer(stopWords);
        }
    },
    BULGARIAN {
        @Override
        protected Analyzer build() {
            return new BulgarianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new BulgarianAnalyzer(stopWords);
        }
    },
    CATALAN {
        @Override
        protected Analyzer build() {
            return new CatalanAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new CatalanAnalyzer(stopWords);
        }
    },
    CHINESE() {
        @Override
        protected Analyzer build() {
            return new StandardAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new StandardAnalyzer(stopWords);
        }
    },
    CJK {
        @Override
        protected Analyzer build() {
            return new CJKAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new CJKAnalyzer(stopWords);
        }
    },
    CZECH {
        @Override
        protected Analyzer build() {
            return new CzechAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new CzechAnalyzer(stopWords);
        }
    },
    DUTCH {
        @Override
        protected Analyzer build() {
            return new DutchAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new DutchAnalyzer(stopWords);
        }
    },
    DANISH {
        @Override
        protected Analyzer build() {
            return new DanishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new DanishAnalyzer(stopWords);
        }
    },
    ENGLISH {
        @Override
        protected Analyzer build() {
            return new EnglishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new EnglishAnalyzer(stopWords);
        }
    },
    FINNISH {
        @Override
        protected Analyzer build() {
            return new FinnishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new FinnishAnalyzer(stopWords);
        }
    },
    FRENCH {
        @Override
        protected Analyzer build() {
            return new FrenchAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new FrenchAnalyzer(stopWords);
        }
    },
    GALICIAN {
        @Override
        protected Analyzer build() {
            return new GalicianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new GalicianAnalyzer(stopWords);
        }
    },
    GERMAN {
        @Override
        protected Analyzer build() {
            return new GermanAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new GermanAnalyzer(stopWords);
        }
    },
    GREEK {
        @Override
        protected Analyzer build() {
            return new GreekAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new GreekAnalyzer(stopWords);
        }
    },
    HINDI {
        @Override
        protected Analyzer build() {
            return new HindiAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new HindiAnalyzer(stopWords);
        }
    },
    HUNGARIAN {
        @Override
        protected Analyzer build() {
            return new HungarianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new HungarianAnalyzer(stopWords);
        }
    },
    INDONESIAN {
        @Override
        protected Analyzer build() {
            return new IndonesianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new IndonesianAnalyzer(stopWords);
        }
    },
    IRISH {
        @Override
        protected Analyzer build() {
            return new IrishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new IrishAnalyzer(stopWords);
        }
    },
    ITALIAN {
        @Override
        protected Analyzer build() {
            return new ItalianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new ItalianAnalyzer(stopWords);
        }
    },
    LATVIAN {
        @Override
        protected Analyzer build() {
            return new LatvianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new LatvianAnalyzer(stopWords);
        }
    },
    LITHUANIAN {
        @Override
        protected Analyzer build() {
            return new LithuanianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new LithuanianAnalyzer(stopWords);
        }
    },
    NORWEGIAN {
        @Override
        protected Analyzer build() {
            return new NorwegianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new NorwegianAnalyzer(stopWords);
        }
    },
    PERSIAN {
        @Override
        protected Analyzer build() {
            return new PersianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new PersianAnalyzer(stopWords);
        }
    },
    PORTUGUESE {
        @Override
        protected Analyzer build() {
            return new PortugueseAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new PortugueseAnalyzer(stopWords);
        }
    },
    ROMANIAN {
        @Override
        protected Analyzer build() {
            return new RomanianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new RomanianAnalyzer(stopWords);
        }
    },
    RUSSIAN {
        @Override
        protected Analyzer build() {
            return new RussianAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new RussianAnalyzer(stopWords);
        }
    },
    SORANI {
        @Override
        protected Analyzer build() {
            return new SoraniAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new SoraniAnalyzer(stopWords);
        }
    },
    SPANISH {
        @Override
        protected Analyzer build() {
            return new SpanishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new SpanishAnalyzer(stopWords);
        }
    },
    SWEDISH {
        @Override
        protected Analyzer build() {
            return new SwedishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new SwedishAnalyzer(stopWords);
        }
    },
    TURKISH {
        @Override
        protected Analyzer build() {
            return new TurkishAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new TurkishAnalyzer(stopWords);
        }
    },
    THAI {
        @Override
        protected Analyzer build() {
            return new ThaiAnalyzer();
        }

        @Override
        protected Analyzer build(CharArraySet stopWords) {
            return new ThaiAnalyzer(stopWords);
        }
    };

    /**
     * Loads the CUSTOM_LANG_STOP_SET in a lazy fashion once the outer class
     * accesses the static final set the first time.
     * <p>
     * IMPORTANT: Note that custom stopword files should not be changed once you start analyzing the data; otherwise you
     * will need to re-index the data to ensure that the indexing and searching are done correctly
     */
    protected static class DefaultStopSetHolder {

        // put your custom stopwords into "org.hawkore.ignite.lucene.schema.analysis" package
        protected static final Map<String, CharArraySet> CUSTOM_LANG_STOP_SET;

        static {
            CUSTOM_LANG_STOP_SET = new HashMap<>();
            for (StandardAnalyzers std : StandardAnalyzers.values()) {
                try {
                    CUSTOM_LANG_STOP_SET.put(std.name().toLowerCase(Locale.ROOT),
                        // load custom stopwords
                        WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(StandardAnalyzers.class,
                            std.name().toLowerCase(Locale.ROOT) + "_stop.txt", StandardCharsets.UTF_8)));
                } catch (Exception e) {
                    // NOP
                }
            }
        }
    }

    /**
     * Returns a new instance of the defined {@link Analyzer}.
     *
     * @return a new analyzer
     */
    abstract Analyzer build();

    /**
     * Returns a new instance of the defined {@link Analyzer} with custom stop words file.
     *
     * @return a new analyzer
     */
    abstract Analyzer build(CharArraySet stopWords);

    /**
     * Returns the prebuilt {@link Analyzer} identified by the specified name, or {@code null} if there is no such
     * {@link Analyzer}.
     *
     * @param name
     *     a prebuilt {@link Analyzer} name
     * @return the analyzer, or {@code null} if there is no such analyzer
     */
    public static Analyzer get(String name) {
        try {
            return valueOf(name.toUpperCase(Locale.ROOT)).get();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns the {@link Analyzer} defined by this.
     *
     * @return the analyzer
     */
    public Analyzer get() {
        CharArraySet stopWords = DefaultStopSetHolder.CUSTOM_LANG_STOP_SET.get(this.name().toLowerCase(Locale.ROOT));
        if (stopWords != null) {
            // use custom stop words file
            return build(stopWords);
        } else {
            // use prebuilt default stop words
            return build();
        }
    }
}
