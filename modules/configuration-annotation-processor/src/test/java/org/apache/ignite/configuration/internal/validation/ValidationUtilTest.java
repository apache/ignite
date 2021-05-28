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

package org.apache.ignite.configuration.internal.validation;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.SuperRoot;
import org.apache.ignite.configuration.internal.asm.ConfigurationAsmGenerator;
import org.apache.ignite.configuration.internal.util.ConfigurationUtil;
import org.apache.ignite.configuration.storage.ConfigurationType;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** */
public class ValidationUtilTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(ValidatedRootConfigurationSchema.class);
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface LeafValidation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface InnerValidation {
    }

    /** */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface NamedListValidation {
    }

    /** */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class ValidatedRootConfigurationSchema {
        /** */
        @InnerValidation
        @ConfigValue
        public ValidatedChildConfigurationSchema child;

        /** */
        @NamedListValidation
        @NamedConfigValue
        public ValidatedChildConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ValidatedChildConfigurationSchema {
        /** */
        @LeafValidation
        @Value(hasDefault = true)
        public String str = "foo";
    }

    /** */
    private InnerNode root;

    /** */
    @BeforeEach
    public void before() {
        root = cgen.instantiateNode(ValidatedRootConfigurationSchema.class);

        ConfigurationUtil.addDefaults(root, root);
    }

    /** */
    @Test
    public void validateLeafNode() throws Exception {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<LeafValidation, String> validator = new Validator<>() {
            @Override public void validate(LeafValidation annotation, ValidationContext<String> ctx) {
                assertEquals("root.child.str", ctx.currentKey());

                assertEquals("foo", ctx.getOldValue());
                assertEquals("foo", ctx.getNewValue());

                ctx.addIssue(new ValidationIssue("bar"));
            }
        };

        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = Map.of(LeafValidation.class, Set.of(validator));

        List<ValidationIssue> issues = ValidationUtil.validate(rootsNode, rootsNode, null, new HashMap<>(), validators);

        assertEquals(1, issues.size());

        assertEquals("bar", issues.get(0).message());
    }

    /** */
    @Test
    public void validateInnerNode() throws Exception {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<InnerValidation, ValidatedChildView> validator = new Validator<>() {
            @Override public void validate(InnerValidation annotation, ValidationContext<ValidatedChildView> ctx) {
                assertEquals("root.child", ctx.currentKey());

                assertEquals("foo", ctx.getOldValue().str());
                assertEquals("foo", ctx.getNewValue().str());

                ctx.addIssue(new ValidationIssue("bar"));
            }
        };

        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = Map.of(InnerValidation.class, Set.of(validator));

        List<ValidationIssue> issues = ValidationUtil.validate(rootsNode, rootsNode, null, new HashMap<>(), validators);

        assertEquals(1, issues.size());

        assertEquals("bar", issues.get(0).message());
    }

    /** */
    @Test
    public void validateNamedListNode() throws Exception {
        var rootsNode = new SuperRoot(key -> null, Map.of(ValidatedRootConfiguration.KEY, root));

        Validator<NamedListValidation, NamedListView<?>> validator = new Validator<>() {
            @Override public void validate(NamedListValidation annotation, ValidationContext<NamedListView<?>> ctx) {
                assertEquals("root.elements", ctx.currentKey());

                assertEquals(emptySet(), ctx.getOldValue().namedListKeys());
                assertEquals(emptySet(), ctx.getNewValue().namedListKeys());

                ctx.addIssue(new ValidationIssue("bar"));
            }
        };

        Map<Class<? extends Annotation>, Set<Validator<?, ?>>> validators = Map.of(NamedListValidation.class, Set.of(validator));

        List<ValidationIssue> issues = ValidationUtil.validate(rootsNode, rootsNode, null, new HashMap<>(), validators);

        assertEquals(1, issues.size());

        assertEquals("bar", issues.get(0).message());
    }
}
