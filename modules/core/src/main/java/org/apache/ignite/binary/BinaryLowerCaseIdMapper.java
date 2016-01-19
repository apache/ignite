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

package org.apache.ignite.binary;

/**
 * ID mapper that uses given strings in lower case to calculate type ID.
 */
public class BinaryLowerCaseIdMapper implements BinaryIdMapper {
    /** Maximum lower-case character. */
    private static final char MAX_LOWER_CASE_CHAR = 0x7e;

    /** Cached lower-case characters. */
    private static final char[] LOWER_CASE_CHARS;

    /**
     * Static initializer.
     */
    static {
        LOWER_CASE_CHARS = new char[MAX_LOWER_CASE_CHAR + 1];

        for (char c = 0; c <= MAX_LOWER_CASE_CHAR; c++)
            LOWER_CASE_CHARS[c] = Character.toLowerCase(c);
    }

    /**
     * Get type ID.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        assert typeName != null;

        return lowerCaseHashCode(typeName, true);
    }

    /**
     * Get field ID.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName) {
        assert fieldName != null;

        return lowerCaseHashCode(fieldName, false);
    }

    /**
     * Routine to calculate string hash code an
     *
     * @param str String.
     * @param type {@code True} if this is type name, false otherwise.
     * @return Hash code for given string converted to lower case.
     */
    private static int lowerCaseHashCode(String str, boolean type) {
        int len = str.length();

        int h = 0;

        for (int i = 0; i < len; i++) {
            int c = str.charAt(i);

            c = c <= MAX_LOWER_CASE_CHAR ? LOWER_CASE_CHARS[c] : Character.toLowerCase(c);

            h = 31 * h + c;
        }

        if (h != 0)
            return h;
        else {
            String what = type ? "type" : "field";

            throw new BinaryObjectException("Binary simple name ID mapper resolved " + what + " ID to zero " +
                "(either change " + what + "'s name or use custom ID mapper) [name=" + str + ']');
        }
    }
}
