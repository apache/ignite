/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.junit.platform.commons.util.CollectionUtils.toStream;
import static org.junit.platform.commons.util.ReflectionUtils.isStatic;

import java.lang.reflect.Field;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.junit.platform.commons.util.ReflectionUtils;

/**
 * {@link ArgumentsProvider} implementation that extracts arguments from a static variable, which name is specified by the
 * {@link VariableSource} annotation.
 */
class VariableArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<VariableSource> {
    private String variableName;

    @Override
    public void accept(VariableSource variableSource) {
        variableName = variableSource.value();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
        Class<?> testClass = context.getRequiredTestClass();

        Field source = testClass.getDeclaredField(variableName);

        if (!isStatic(source)) {
            throw new IllegalArgumentException("Variable marked with @VariableSource must be static: " + variableName);
        }

        source.setAccessible(true);

        Object sourceValue = source.get(null);

        return toStream(sourceValue).map(VariableArgumentsProvider::toArguments);
    }

    /**
     * Converts the given value to {@link Arguments}.
     * Copy-pasted from {@code org.junit.jupiter.params.provider.MethodArgumentsProvider#toArguments}
     */
    private static Arguments toArguments(Object item) {
        // Nothing to do except cast.
        if (item instanceof Arguments) {
            return (Arguments) item;
        }

        // Pass all multidimensional arrays "as is", in contrast to Object[].
        // See https://github.com/junit-team/junit5/issues/1665
        if (ReflectionUtils.isMultidimensionalArray(item)) {
            return arguments(item);
        }

        // Special treatment for one-dimensional reference arrays.
        // See https://github.com/junit-team/junit5/issues/1665
        if (item instanceof Object[]) {
            return arguments((Object[]) item);
        }

        // Pass everything else "as is".
        return arguments(item);
    }
}
