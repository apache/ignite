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
package org.apache.ignite.configuration.processor.internal;

import com.google.testing.compile.CompilationSubject;
import com.squareup.javapoet.ClassName;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.apache.ignite.configuration.processor.internal.HasFieldMatcher.hasFields;
import static org.apache.ignite.configuration.processor.internal.HasMethodMatcher.hasMethods;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for basic code generation scenarios.
 */
public class ProcessorTest extends AbstractProcessorTest {
    /**
     * The simplest test for code generation.
     */
    @Test
    public void test() {
        final String packageName = "org.apache.ignite.configuration.processor.internal";

        final ClassName testConfigurationSchema = ClassName.get(packageName, "TestConfigurationSchema");

        final BatchCompilation batch = batchCompile(testConfigurationSchema);

        CompilationSubject.assertThat(batch.getCompilationStatus()).succeeded();

        assertEquals(7, batch.generated().size());

        final ConfigSet classSet = batch.getBySchema(testConfigurationSchema);

        assertTrue(classSet.allGenerated());

        MatcherAssert.assertThat(
            classSet.getViewClass(),
            hasFields(
                "value1", Types.STRING,
                "primitiveLong", Types.LONG,
                "boxedLong", Types.LONG,
                "primitiveInt", Types.INT,
                "boxedInt", Types.INT
            )
        );

        MatcherAssert.assertThat(
            classSet.getViewClass(),
            hasMethods(
                "value1()", Types.STRING,
                "primitiveLong()", Types.LONG,
                "boxedLong()", Types.LONG,
                "primitiveInt()", Types.INT,
                "boxedInt()", Types.INT
            )
        );

        MatcherAssert.assertThat(
            classSet.getInitClass(),
            hasFields(
                "value1", Types.STRING,
                "primitiveLong", Types.LONG,
                "boxedLong", Types.LONG,
                "primitiveInt", Types.INT,
                "boxedInt", Types.INT
            )
        );

        String initTypeName = Types.typeName(packageName, "InitTest");
        
        MatcherAssert.assertThat(
            classSet.getInitClass(),
            hasMethods(
                "value1()", Types.STRING,
                "primitiveLong()", Types.LONG,
                "boxedLong()", Types.LONG,
                "primitiveInt()", Types.INT,
                "boxedInt()", Types.INT,
                "withValue1(java.lang.String)", initTypeName,
                "withPrimitiveLong(java.lang.Long)", initTypeName,
                "withBoxedLong(java.lang.Long)", initTypeName,
                "withPrimitiveInt(java.lang.Integer)", initTypeName,
                "withBoxedInt(java.lang.Integer)", initTypeName
            )
        );
    }

}
