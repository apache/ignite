/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.checkstyle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

/**
 * Rule that check Ignite abbervations used through project source code.
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/Abbreviation+Rules#AbbreviationRules-VariableAbbreviation">
 *     Ignite abbrevation rules</a>
 */
public class IgniteAbbrevationsRule extends AbstractCheck {
    /** File with abbrevations. */
    public static final String ABBREVS_FILE = "/abbrevations.csv";

    /** */
    public static final char DELIM = ',';

    /** */
    private static final int[] TOKENS = new int[] {
        TokenTypes.VARIABLE_DEF, TokenTypes.PATTERN_VARIABLE_DEF
    };

    /**
     * Key is wrong term that should be replaced with abbrevations.
     * Value is array of possible substitute to generate self-explained error message.
     */
    private final Map<String, String[]> abbrevs = new HashMap<>();

    /** */
    public IgniteAbbrevationsRule() {
        InputStream stream = getClass().getResourceAsStream(ABBREVS_FILE);

        try (BufferedReader br =
                 new BufferedReader(new InputStreamReader(stream))) {
            String line;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;

                line = line.toLowerCase();

                int firstDelim = line.indexOf(DELIM);

                assert firstDelim > 0;

                String term = line.substring(0, firstDelim);
                String[] substitutions = line.substring(firstDelim + 1).split("" + DELIM);

                assert substitutions.length > 0;

                abbrevs.put(term, substitutions);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST token = ast.findFirstToken(TokenTypes.IDENT);

        String varName = token.getText();

        List<String> words = words(varName);

        for (String word : words) {
            if (abbrevs.containsKey(word.toLowerCase())) {
                String correct = String.join(", ", abbrevs.get(word.toLowerCase()));

                log(
                    token.getLineNo(),
                    "Abbrevation should be used for {0}! Please, use {1}, instead of {2}",
                    varName,
                    correct,
                    word
                );
            }
        }
    }

    public static List<String> words(String varName) {
        if (varName.indexOf('_') != -1)
            return Arrays.asList(varName.split("_"));

        List<String> words = new ArrayList<>();

        int start = 0;
        int finish = 0;

        for (int i = 0; i < varName.length(); i++) {
            if (Character.isUpperCase(varName.charAt(i)))
                finish = i;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public int[] getDefaultTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getAcceptableTokens() {
        return TOKENS.clone();
    }

    /** {@inheritDoc} */
    @Override public int[] getRequiredTokens() {
        return TOKENS.clone();
    }
}
