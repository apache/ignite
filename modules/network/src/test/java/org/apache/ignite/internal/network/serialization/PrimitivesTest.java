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

package org.apache.ignite.internal.network.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link Primitives}.
 */
class PrimitivesTest {
    @ParameterizedTest
    @MethodSource("primitiveByteWidths")
    void widthInBytesIsCorrectForPrimitiveTypes(Class<?> type, int expectedWidth) {
        assertThat(Primitives.widthInBytes(type), is(expectedWidth));
    }

    private static Stream<Arguments> primitiveByteWidths() {
        return Stream.of(
                Arguments.of(byte.class, 1),
                Arguments.of(short.class, 2),
                Arguments.of(int.class, 4),
                Arguments.of(long.class, 8),
                Arguments.of(float.class, 4),
                Arguments.of(double.class, 8),
                Arguments.of(char.class, 2),
                Arguments.of(boolean.class, 1)
        );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void widthInBytesThrowsForNonPrimitiveTypes() {
        assertThrows(IllegalArgumentException.class, () -> Primitives.widthInBytes(Object.class));
    }
}
