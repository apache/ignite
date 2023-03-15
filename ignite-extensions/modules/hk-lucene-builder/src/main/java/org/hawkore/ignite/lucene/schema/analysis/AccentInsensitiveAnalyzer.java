/*
 * Copyright (C) 2018 HAWKORE, S.L. (http://hawkore.com)
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

import java.lang.reflect.Method;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.hawkore.ignite.lucene.IndexException;

/**
 * 
 * AccentInsensitiveAnalyzer
 *
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 *
 */
public class AccentInsensitiveAnalyzer extends Analyzer {
    
    private final Analyzer subAnalyzer;

    private final Method createComponents;
    
    /**
     * 
     * @param subAnalyzer
     */
    public AccentInsensitiveAnalyzer(Analyzer subAnalyzer) {
        try {
            this.subAnalyzer = subAnalyzer;
            this.createComponents = subAnalyzer.getClass().getDeclaredMethod("createComponents", String.class);
            if (!createComponents.isAccessible()) {
                createComponents.setAccessible(true);
            }
        } catch (Exception e) {
            throw new IndexException(e);
        }
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        try {

            final TokenStreamComponents source = (TokenStreamComponents) createComponents.invoke(this.subAnalyzer,
                fieldName);

            TokenStream tokenStream = source.getTokenStream();
            tokenStream = new ASCIIFoldingFilter(tokenStream);
            return new TokenStreamComponents(source.getSource(), tokenStream);

        } catch (Exception e) {
            throw new IndexException(e);
        }
    }

}