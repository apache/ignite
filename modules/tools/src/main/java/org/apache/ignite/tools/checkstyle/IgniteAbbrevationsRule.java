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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.puppycrawl.tools.checkstyle.api.AbstractCheck;

/**
 * Rule that check Ignite abbervations used through project source code.
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/Abbreviation+Rules#AbbreviationRules-VariableAbbreviation">
 *     Ignite abbrevation rules</a>
 */
public class IgniteAbbrevationsRule extends AbstractCheck {
    /** */
    public static final String ABBREVS_FILE = "abbrevations.csv";

    /** */
    public static final char DELIM = ',';

    /**
     * Key is wrong term that should be replaced with abbrevations.
     * Value is array of possible substitute to generate self-explained error message.
     */
    private final Map<String, String[]> abbrevs = new HashMap<>();

    /** */
    public IgniteAbbrevationsRule() {
        try {
            List<String> strs =
                Files.readAllLines(new File(getClass().getClassLoader().getResource(ABBREVS_FILE).getFile()).toPath());

            for (String str : strs) {
                int firstDelim = str.indexOf(DELIM);

                assert firstDelim > 0;

                String term = str.substring(0, firstDelim);
                String[] substitutions = str.substring(firstDelim + 1).split("" + DELIM);

                System.out.println(term);

                assert substitutions.length > 0;

                abbrevs.put(term, substitutions);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        new IgniteAbbrevationsRule();
    }

    @Override public int[] getDefaultTokens() {
        return new int[0];
    }

    @Override public int[] getAcceptableTokens() {
        return new int[0];
    }

    @Override public int[] getRequiredTokens() {
        return new int[0];
    }
}
