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

package org.apache.ignite.internal.testframework;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.BitSet;
import java.util.Random;
import java.util.function.BooleanSupplier;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Utility class for tests.
 */
public final class IgniteTestUtils {
    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteInternalException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteInternalException("Modification of static final field through reflection.");

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj Object to set field value to.
     * @param cls Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteInternalException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            boolean accessible = field.isAccessible();

            if (!accessible)
                field.setAccessible(true);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /**
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic)
                throw new IgniteInternalException("Modification of static final field through reflection.");

            if (isFinal) {
                Field modifiersField = Field.class.getDeclaredField("modifiers");

                modifiersField.setAccessible(true);

                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            }

            field.set(obj, val);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Checks whether runnable throws exception, which is itself of a specified
     * class, or has a cause of the specified class.
     *
     * @param run Runnable to check.
     * @param cls Expected exception class.
     * @return Thrown throwable.
     */
    @Nullable public static Throwable assertThrowsWithCause(
        @NotNull Runnable run,
        @NotNull Class<? extends Throwable> cls
    ) {
        try {
            run.run();
        }
        catch (Throwable e) {
            if (!hasCause(e, cls, null))
                fail("Exception is neither of a specified class, nor has a cause of the specified class: " + cls, e);

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * <p>
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param t Throwable to check.
     * @param cls Cause classes to check.
     * @param msg Message text that should be in cause (if {@code null}, message won't be checked).
     * @return {@code True} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    private static boolean hasCause(
        @NotNull Throwable t,
        @NotNull Class<?> cls,
        @Nullable String msg
    ) {
        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass())) {
                if (msg != null) {
                    if (th.getMessage() != null && th.getMessage().contains(msg))
                        return true;
                    else
                        continue;
                }

                return true;
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, cls, msg))
                    return true;
            }

            if (th.getCause() == th)
                break;
        }

        return false;
    }

    /**
     * Waits for the condition.
     *
     * @param cond Condition.
     * @param timeoutMillis Timeout in milliseconds.
     * @return {@code True} if the condition was satisfied within the timeout.
     * @throws InterruptedException If waiting was interrupted.
     */
    @SuppressWarnings("BusyWait") public static boolean waitForCondition(BooleanSupplier cond, long timeoutMillis)
        throws InterruptedException {
        long stop = System.currentTimeMillis() + timeoutMillis;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            sleep(50);
        }

        return false;
    }

    /**
     * @param rnd Random generator.
     * @param bits Amount of bits in bitset.
     * @return Random BitSet.
     */
    public static BitSet randomBitSet(Random rnd, int bits) {
        BitSet set = new BitSet();

        for (int i = 0; i < bits; i++) {
            if (rnd.nextBoolean())
                set.set(i);
        }

        return set;
    }

    /**
     * @param rnd Random generator.
     * @param len Byte array length.
     * @return Random byte array.
     */
    public static byte[] randomBytes(Random rnd, int len) {
        byte[] data = new byte[len];
        rnd.nextBytes(data);

        return data;
    }

    /**
     * @param rnd Random generator.
     * @param len String length.
     * @return Random string.
     */
    public static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder();

        while (sb.length() < len) {
            char pt = (char)rnd.nextInt(Character.MAX_VALUE + 1);

            if (Character.isDefined(pt) &&
                Character.getType(pt) != Character.PRIVATE_USE &&
                !Character.isSurrogate(pt))
                sb.append(pt);
        }

        return sb.toString();
    }
}
