/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.internal;

/**
 * Sometime instead of reflection, better performance.
 */
public class Updaters {

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U> IntegerFieldUpdater<U> newIntegerFieldUpdater(final Class<? super U> tClass,
        final String fieldName) {
        try {
//            if (UnsafeUtil.hasUnsafe()) {
//                return new UnsafeIntegerFieldUpdater<>(UnsafeUtil.getUnsafeAccessor().getUnsafe(), tClass, fieldName);
//            } else {
//                return new ReflectionIntegerFieldUpdater<>(tClass, fieldName);
//            }

            return new ReflectionIntegerFieldUpdater<>(tClass, fieldName);
        }
        catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U> LongFieldUpdater<U> newLongFieldUpdater(final Class<? super U> tClass, final String fieldName) {
        try {
//            if (UnsafeUtil.hasUnsafe()) {
//                return new UnsafeLongFieldUpdater<>(UnsafeUtil.getUnsafeAccessor().getUnsafe(), tClass, fieldName);
//            } else {
//                return new ReflectionLongFieldUpdater<>(tClass, fieldName);
//            }

            return new ReflectionLongFieldUpdater<>(tClass, fieldName);
        }
        catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Creates and returns an updater for objects with the given field.
     *
     * @param tClass the class of the objects holding the field.
     * @param fieldName the name of the field to be updated.
     */
    public static <U, W> ReferenceFieldUpdater<U, W> newReferenceFieldUpdater(final Class<? super U> tClass,
        final String fieldName) {
        try {
//            if (UnsafeUtil.hasUnsafe()) {
//                return new UnsafeReferenceFieldUpdater<>(UnsafeUtil.getUnsafeAccessor().getUnsafe(), tClass, fieldName);
//            } else {
//                return new ReflectionReferenceFieldUpdater<>(tClass, fieldName);
//            }

            return new ReflectionReferenceFieldUpdater<>(tClass, fieldName);
        }
        catch (final Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
