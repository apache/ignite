/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.model;

import java.util.Random;

/**
 *
 */
public class ModelUtil {
    /** */
    static final String ALPHABETH = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890_";

    /**
     * Generate random alphabetical string.
     *
     * @param rnd Random object.
     * @param minLen Minimum length of string.
     * @param maxLen Maximal length of string.
     * @return Random string object.
     */
    public static String randomString(Random rnd, int minLen, int maxLen) {
        assert minLen >= 0 : "minLen >= 0";
        assert maxLen >= minLen : "maxLen >= minLen";

        int len = maxLen == minLen ? minLen : minLen + rnd.nextInt(maxLen - minLen);

        StringBuilder b = new StringBuilder(len);

        for (int i = 0; i < len; i++)
            b.append(ALPHABETH.charAt(rnd.nextInt(ALPHABETH.length())));

        return b.toString();
    }

    private ModelUtil() {
    }



}
