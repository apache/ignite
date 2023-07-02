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
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;
import org.hawkore.ignite.lucene.IndexException;
import org.tartarus.snowball.SnowballProgram;

/**
 * Version of {@link org.apache.lucene.analysis.snowball.SnowballFilter} modified to be compatible with
 * {@link org.tartarus.snowball.SnowballStemmer} 1.3.0.581.1, imposed by SASI indexes.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class SnowballFilter extends TokenFilter {

    private final SnowballProgram stemmer;

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

    /**
     * Construct the a new snowball filter for the specified language.
     *
     * The supported languages are English, French, Spanish, Portuguese, Italian, Romanian, German, Dutch, Swedish,
     * Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian, Basque and Catalan.
     *
     * @param stream the input tokens stream to be stemmed
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Hungarian and Turkish. Basque and
     * Catalan.
     */
    public SnowballFilter(TokenStream stream, String language) {
        super(stream);
        try {
            stemmer = Class.forName("org.tartarus.snowball.ext." + StringUtils.capitalize(language.toLowerCase()) + "Stemmer")
                           .asSubclass(SnowballProgram.class)
                           .newInstance();
        } catch (Exception e) {
            throw new IndexException(e, "The specified language '{}' is not valid", language);
        }
    }

    /** {@inheritDoc} */
    @Override
    public final boolean incrementToken() throws IOException {
        if (input.incrementToken()) {
            if (!keywordAttr.isKeyword()) {
                char termBuffer[] = termAtt.buffer();
                final int length = termAtt.length();
                stemmer.setCurrent(new String(Arrays.copyOf(termBuffer, length)));
                if (stemmer.stem()) {
                    final char finalTerm[] = stemmer.getCurrent().toCharArray();
                    final int newLength = finalTerm.length;
                    termAtt.copyBuffer(finalTerm, 0, newLength);
                } else {
                    termAtt.setLength(length);
                }
            }
            return true;
        }
        return false;
    }
}