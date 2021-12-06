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

package org.apache.ignite.internal.configuration;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompoundModuleTest {
    @Mock
    private RootKey<?, ?> rootKeyA;
    @Mock
    private RootKey<?, ?> rootKeyB;

    @Mock
    private Validator<AnnotationA, ?> validatorA;
    @Mock
    private Validator<AnnotationB, ?> validatorB;

    @Mock
    private ConfigurationModule moduleA;
    @Mock
    private ConfigurationModule moduleB;

    private ConfigurationModule compound;

    @BeforeEach
    void createCompoundModule() {
        compound = new CompoundModule(ConfigurationType.LOCAL, List.of(moduleA, moduleB));
    }

    @Test
    void returnsTypePassedViaConstructor() {
        var compound = new CompoundModule(ConfigurationType.LOCAL, emptyList());

        assertThat(compound.type(), is(ConfigurationType.LOCAL));
    }

    @Test
    void returnsUnionOfRootKeysOfItsModules() {
        when(moduleA.rootKeys()).thenReturn(Set.of(rootKeyA));
        when(moduleB.rootKeys()).thenReturn(Set.of(rootKeyB));

        assertThat(compound.rootKeys(), containsInAnyOrder(rootKeyA, rootKeyB));
    }

    @Test
    void providesUnionOfValidatorsMapsOfItsModulesPerKey() {
        when(moduleA.validators()).thenReturn(Map.of(AnnotationA.class, Set.of(validatorA)));
        when(moduleB.validators()).thenReturn(Map.of(AnnotationB.class, Set.of(validatorB)));

        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators = compound.validators();

        assertThat(validators, hasEntry(AnnotationA.class, Set.of(validatorA)));
        assertThat(validators, hasEntry(AnnotationB.class, Set.of(validatorB)));
    }

    @Test
    void mergesValidatorsUnderTheSameKey() {
        when(moduleA.validators()).thenReturn(Map.of(AnnotationA.class, Set.of(validatorA)));
        when(moduleB.validators()).thenReturn(Map.of(AnnotationA.class, Set.of(validatorB)));

        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators = compound.validators();

        assertThat(validators, hasEntry(AnnotationA.class, Set.of(validatorA, validatorB)));
    }

    @Test
    void returnsUnionOfInternalSchemaExtensionsOfItsModules() {
        when(moduleA.internalSchemaExtensions()).thenReturn(Set.of(ExtensionA.class));
        when(moduleB.internalSchemaExtensions()).thenReturn(Set.of(ExtensionB.class));

        assertThat(compound.internalSchemaExtensions(), containsInAnyOrder(ExtensionA.class, ExtensionB.class));
    }

    @Test
    void returnsUnionOfPolymorphicSchemaExtensionsOfItsModules() {
        when(moduleA.polymorphicSchemaExtensions()).thenReturn(Set.of(ExtensionA.class));
        when(moduleB.polymorphicSchemaExtensions()).thenReturn(Set.of(ExtensionB.class));

        assertThat(compound.polymorphicSchemaExtensions(), containsInAnyOrder(ExtensionA.class, ExtensionB.class));
    }

    private @interface AnnotationA {
    }

    private @interface AnnotationB {
    }

    private static class ExtensionA {
    }

    private static class ExtensionB {
    }
}
