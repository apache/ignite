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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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

    /** File with exclusions. */
    public static final String ABBREVS_EXCL_FILE = "/abbrevations-exclude.txt";

    /** */
    public static final char DELIM = ',';

    /** */
    private static final int[] TOKENS = new int[] {
        TokenTypes.VARIABLE_DEF, TokenTypes.PATTERN_VARIABLE_DEF
    };

    /**
     * Key is wrong term that should be replaced with abbrevations.
     * Value possible substitions to generate self-explained error message.
     */
    private static final Map<String, String> ABBREVS = new HashMap<>();

    /** Exclusions. */
    private static final Set<String> EXCL = new HashSet<>();

    static {
        forEachLine(ABBREVS_FILE, line -> {
            line = line.toLowerCase();

            int firstDelim = line.indexOf(DELIM);

            assert firstDelim > 0;

            String term = line.substring(0, firstDelim);
            String[] substitutions = line.substring(firstDelim + 1).split("" + DELIM);

            assert substitutions.length > 0;

            ABBREVS.put(term, String.join(", ", substitutions));
        });

        forEachLine(ABBREVS_EXCL_FILE, EXCL::add);
    }

    /** {@inheritDoc} */
    @Override public void visitToken(DetailAST ast) {
        DetailAST parent = ast.getParent();

        if (parent.getType() == TokenTypes.OBJBLOCK)
            return;

        DetailAST token = ast.findFirstToken(TokenTypes.IDENT);

        String varName = token.getText();

        if (EXCL.contains(varName))
            return;

        List<String> words = words(varName);

        for (String word : words) {
            if (ABBREVS.containsKey(word.toLowerCase())) {
                log(
                    token.getLineNo(),
                    "Abbrevation should be used for {0}! Please, use {1}, instead of {2}",
                    varName,
                    ABBREVS.get(word.toLowerCase()),
                    word
                );
            }
        }
    }

    /**
     * @param varName Variable name.
     * @return Words list.
     */
    public static List<String> words(String varName) {
        if (varName.indexOf('_') != -1)
            return Arrays.asList(varName.split("_"));

        List<String> words = new ArrayList<>();

        int start = 0;
        boolean allUpper = Character.isUpperCase(varName.charAt(0));

        for (int i = 1; i < varName.length(); i++) {
            if (Character.isUpperCase(varName.charAt(i))) {
                if (allUpper)
                    continue;

                words.add(varName.substring(start, i));
                start = i;
                allUpper = true;
            }
            else
                allUpper = false;
        }

        words.add(varName.substring(start));

        return words;
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

    /** */
    private static void forEachLine(String file, Consumer<String> lineProc) {
        InputStream stream = IgniteAbbrevationsRule.class.getResourceAsStream(file);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream))) {
            String line;

            while ((line = br.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;

                lineProc.accept(line);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
