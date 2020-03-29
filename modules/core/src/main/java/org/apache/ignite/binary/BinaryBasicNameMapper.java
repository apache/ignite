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

/**
 * Base binary name mapper implementation.
 */
public class BinaryBasicNameMapper implements BinaryNameMapper {
    /** Default use simple name flag setting. */
    public static final boolean DFLT_SIMPLE_NAME = false;

    /** */
    private boolean isSimpleName = DFLT_SIMPLE_NAME;

    /**
     * Default constructor.
     */
    public BinaryBasicNameMapper() {
        // No-op.
    }

    /**
     * @param isSimpleName Whether to use simple (no package) name of class or not.
     * <p>
     * Defaults to {@link #DFLT_SIMPLE_NAME}.
     */
    public BinaryBasicNameMapper(boolean isSimpleName) {
        this.isSimpleName = isSimpleName;
    }

    /**
     * Gets whether to use simple name of class or not.
     *
     * @return Whether to use simple name of class or not.
     */
    public boolean isSimpleName() {
        return isSimpleName;
    }

    /**
     * Sets whether to use simple name of class or not.
     *
     * @param isSimpleName Whether to use simple name of class or not.
     * @return {@code this} for chaining.
     */
    public BinaryBasicNameMapper setSimpleName(boolean isSimpleName) {
        this.isSimpleName = isSimpleName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String typeName(String clsName) {
        A.notNull(clsName, "clsName");

        return isSimpleName ? simpleName(clsName) : clsName;
    }

    /** {@inheritDoc} */
    @Override public String fieldName(String fieldName) {
        A.notNull(fieldName, "fieldName");

        return fieldName;
    }

    /**
     * @param clsName Class name.
     * @return Type name.
     */
    private static String simpleName(String clsName) {
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
            idx = clsName.lastIndexOf('+');   // .NET inner class.

        if (idx < 0)
            idx = clsName.lastIndexOf('.');

        return idx >= 0 ? clsName.substring(idx + 1) : clsName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof BinaryBasicNameMapper))
            return false;

        BinaryBasicNameMapper mapper = (BinaryBasicNameMapper)o;

        if (isSimpleName != mapper.isSimpleName)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (isSimpleName ? 1 : 0);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "BinaryBaseNameMapper [isSimpleName=" + isSimpleName + ']';
    }
}
