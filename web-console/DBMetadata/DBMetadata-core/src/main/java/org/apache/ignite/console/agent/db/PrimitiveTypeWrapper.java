/*
 *  Copyright 2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.console.agent.db;

/**
 * @author Jeff Butler
 */
public class PrimitiveTypeWrapper extends FullyQualifiedJavaType {
    private static PrimitiveTypeWrapper booleanInstance;
    private static PrimitiveTypeWrapper byteInstance;
    private static PrimitiveTypeWrapper characterInstance;
    private static PrimitiveTypeWrapper doubleInstance;
    private static PrimitiveTypeWrapper floatInstance;
    private static PrimitiveTypeWrapper integerInstance;
    private static PrimitiveTypeWrapper longInstance;
    private static PrimitiveTypeWrapper shortInstance;

    private String toPrimitiveMethod;

    /**
     * Use the static getXXXInstance methods to gain access to one of the type
     * wrappers.
     *
     * @param fullyQualifiedName fully qualified name of the wrapper type
     * @param toPrimitiveMethod  the method that returns the wrapped primitive
     */
    private PrimitiveTypeWrapper(String fullyQualifiedName,
                                 String toPrimitiveMethod) {
        super(fullyQualifiedName);
        this.toPrimitiveMethod = toPrimitiveMethod;
    }

    public String getToPrimitiveMethod() {
        return toPrimitiveMethod;
    }

    public static PrimitiveTypeWrapper getBooleanInstance() {
        if (booleanInstance == null) {
            booleanInstance = new PrimitiveTypeWrapper("java.lang.Boolean",
                    "booleanValue()");
        }

        return booleanInstance;
    }

    public static PrimitiveTypeWrapper getByteInstance() {
        if (byteInstance == null) {
            byteInstance = new PrimitiveTypeWrapper("java.lang.Byte",
                    "byteValue()");
        }

        return byteInstance;
    }

    public static PrimitiveTypeWrapper getCharacterInstance() {
        if (characterInstance == null) {
            characterInstance = new PrimitiveTypeWrapper("java.lang.Character",
                    "charValue()");
        }

        return characterInstance;
    }

    public static PrimitiveTypeWrapper getDoubleInstance() {
        if (doubleInstance == null) {
            doubleInstance = new PrimitiveTypeWrapper("java.lang.Double",
                    "doubleValue()");
        }

        return doubleInstance;
    }

    public static PrimitiveTypeWrapper getFloatInstance() {
        if (floatInstance == null) {
            floatInstance = new PrimitiveTypeWrapper("java.lang.Float",
                    "floatValue()");
        }

        return floatInstance;
    }

    public static PrimitiveTypeWrapper getIntegerInstance() {
        if (integerInstance == null) {
            integerInstance = new PrimitiveTypeWrapper("java.lang.Integer",
                    "intValue()");
        }

        return integerInstance;
    }

    public static PrimitiveTypeWrapper getLongInstance() {
        if (longInstance == null) {
            longInstance = new PrimitiveTypeWrapper("java.lang.Long",
                    "longValue()");
        }

        return longInstance;
    }

    public static PrimitiveTypeWrapper getShortInstance() {
        if (shortInstance == null) {
            shortInstance = new PrimitiveTypeWrapper("java.lang.Short",
                    "shortValue()");
        }

        return shortInstance;
    }
}
