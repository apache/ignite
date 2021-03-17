/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.bytecode;

import java.io.StringWriter;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

import static java.time.ZoneOffset.UTC;

public final class BytecodeUtils {
    private static final AtomicLong CLASS_ID = new AtomicLong();
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private static final Map<Class<?>, Class<?>> PRIMITIVES_TO_WRAPPERS
        = Map.of(boolean.class, Boolean.class,
        byte.class, Byte.class,
        char.class, Character.class,
        double.class, Double.class,
        float.class, Float.class,
        int.class, Integer.class,
        long.class, Long.class,
        short.class, Short.class,
        void.class, Void.class);

    private BytecodeUtils() {
    }

    public static ParameterizedType makeClassName(String baseName, Optional<String> suffix) {
        String className = baseName
            + "_" + suffix.orElseGet(() -> Instant.now().atZone(UTC).format(TIMESTAMP_FORMAT))
            + "_" + CLASS_ID.incrementAndGet();
        String javaClassName = toJavaIdentifierString(className);
        return ParameterizedType.typeFromJavaClassName("com.facebook.presto.$gen." + javaClassName);
    }

    public static ParameterizedType makeClassName(String baseName) {
        return makeClassName(baseName, Optional.empty());
    }

    public static String toJavaIdentifierString(String className) {
        // replace invalid characters with '_'
        return className.codePoints().mapToObj(c -> Character.isJavaIdentifierPart(c) ? c : '_' & 0xFFFF)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }

    public static String dumpBytecodeTree(ClassDefinition classDefinition) {
        StringWriter writer = new StringWriter();
        new DumpBytecodeVisitor(writer).visitClass(classDefinition);
        return writer.toString();
    }

    public static void checkArgument(boolean condition, String errMsg, Object... params) {
        if (!condition)
            throw new IllegalArgumentException(String.format(errMsg, params));
    }

    public static void checkState(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    public static void checkState(
        boolean expression,
        @Nullable String errorMessageTemplate,
        Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalStateException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static <T> Class<T> wrap(Class<T> c) {
        return c.isPrimitive() ? (Class<T>)PRIMITIVES_TO_WRAPPERS.get(c) : c;
    }

    public static String repeat(String string, int count) {
        Objects.requireNonNull(string); // eager for GWT.

        if (count <= 1) {
            checkArgument(count >= 0, "invalid count: %s", count);
            return (count == 0) ? "" : string;
        }

        // IF YOU MODIFY THE CODE HERE, you must update StringsRepeatBenchmark
        final int len = string.length();
        final long longSize = (long)len * (long)count;
        final int size = (int)longSize;
        if (size != longSize) {
            throw new ArrayIndexOutOfBoundsException("Required array size too large: " + longSize);
        }

        final char[] array = new char[size];
        string.getChars(0, len, array, 0);
        int n;
        for (n = len; n < size - n; n <<= 1) {
            System.arraycopy(array, 0, array, n, n);
        }
        System.arraycopy(array, 0, array, n, size - n);
        return new String(array);
    }
}
