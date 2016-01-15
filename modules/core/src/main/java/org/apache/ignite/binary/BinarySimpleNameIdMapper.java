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

import org.apache.ignite.internal.util.typedef.internal.A;
import org.jetbrains.annotations.Nullable;

/**
 * ID mapper that uses a simple name of classes in lower case to calculate type ID.
 */
public class BinarySimpleNameIdMapper implements BinaryIdMapper {
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
     * Wraps custom id mapper.
     *
     * @param mapper Public mapper.
     * @return Wrapper for public mapper.
     */
    public static BinarySimpleNameIdMapper wrap(@Nullable BinaryIdMapper mapper) {
        A.notNull(mapper, "publicMapper");

        return new Wrapper(mapper);
    }

    /**
     * Get type ID.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName) {
        assert typeName != null;

        return lowerCaseHashCode(simpleName(typeName), true);
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

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    public static String simpleName(String clsName) {
        assert clsName != null;

        int idx = clsName.lastIndexOf('$');

        if (idx == clsName.length() - 1)
            // This is a regular (not inner) class name that ends with '$'. Common use case for Scala classes.
            idx = -1;
        else if (idx >= 0) {
            String typeName = clsName.substring(idx + 1);

            try {
                Integer.parseInt(typeName);

                // This is an anonymous class. Don't cut off enclosing class name for it.
                idx = -1;
            }
            catch (NumberFormatException ignore) {
                // This is a lambda class.
                if (clsName.indexOf("$$Lambda$") > 0)
                    idx = -1;
                else
                    return typeName;
            }
        }

        if (idx < 0)
            idx = clsName.lastIndexOf('.');

        return idx >= 0 ? clsName.substring(idx + 1) : clsName;
    }

    /**
     * Wrapping ID mapper.
     */
    private static class Wrapper extends BinarySimpleNameIdMapper {
        /** Delegate. */
        private final BinaryIdMapper mapper;

        /**
         * Constructor.
         *
         * @param mapper Delegate.
         */
        private Wrapper(BinaryIdMapper mapper) {
            assert mapper != null;

            this.mapper = mapper;
        }

        /** {@inheritDoc} */
        @Override public int typeId(String typeName) {
            int id = mapper.typeId(typeName);

            return id != 0 ? id : super.typeId(typeName);
        }

        /** {@inheritDoc} */
        @Override public int fieldId(int typeId, String fieldName) {
            int id = mapper.fieldId(typeId, fieldName);

            return id != 0 ? id : super.fieldId(typeId, fieldName);
        }
    }
}
