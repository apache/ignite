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

package org.apache.ignite.internal.binary;

import org.apache.ignite.binary.BinaryObject;

/**
 * Simple utility class to check package-private class in some tests.
 */
public class BinaryObjectTestUtils {
    /**
     * @param val Value to check.
     * @return {@code True} if {@code val} instance of {@link BinaryObjectOffheapImpl}, {@code false} otherwise.
     */
    public static boolean isBinaryObjectOffheapImplInstance(Object val) {
        return val instanceof BinaryObjectOffheapImpl;
    }

    /**
     * @param obj
     * @return Value of {@link BinaryAbstractIdentityResolver#hashCode(BinaryObject)}
     */
    public static int binaryArrayIdentityResolverHashCode(BinaryObject obj) {
        return BinaryArrayIdentityResolver.instance().hashCode(obj);
    }

    /**
     * @return Binary object instance.
     */
    public static BinaryObject binaryObject() {
        return new BinaryObjectImpl();
    }

    /**
     * @param o Object.
     * @return {@link BinaryObjectImpl#detached()} value.
     */
    public static boolean isDetached(Object o) {
        return ((BinaryObjectImpl)o).detached();
    }
}
