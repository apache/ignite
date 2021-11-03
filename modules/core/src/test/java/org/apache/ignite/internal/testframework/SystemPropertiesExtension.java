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

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit rule that manages usage of {@link WithSystemProperty} annotations.<br> Should be used in {@link ExtendWith}.
 *
 * @see WithSystemProperty
 * @see ExtendWith
 */
public class SystemPropertiesExtension implements
        BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {
    /** JUnit namespace for the extension. */
    private static final Namespace NAMESPACE = Namespace.create(SystemPropertiesExtension.class);

    /** {@inheritDoc} */
    @Override
    public void beforeAll(ExtensionContext ctx) {
        List<WithSystemProperty> newProps = new ArrayList<>();

        for (Class<?> cls = ctx.getRequiredTestClass(); cls != null; cls = cls.getSuperclass()) {
            WithSystemProperty[] props = cls.getAnnotationsByType(WithSystemProperty.class);

            if (props.length > 0) {
                newProps.addAll(Arrays.asList(props));
            }
        }

        if (newProps.isEmpty()) {
            return;
        }

        // Reverse the collected properties to set properties from the superclass first. This may be important
        // if the same property gets overridden in a subclass.
        Collections.reverse(newProps);

        List<Property> oldProps = replaceProperties(newProps);

        ctx.getStore(NAMESPACE).put(ctx.getUniqueId(), oldProps);
    }

    /** {@inheritDoc} */
    @Override
    public void afterAll(ExtensionContext ctx) {
        resetProperties(ctx);
    }

    /** {@inheritDoc} */
    @Override
    public void beforeEach(ExtensionContext ctx) {
        WithSystemProperty[] newProps = ctx.getRequiredTestMethod().getAnnotationsByType(WithSystemProperty.class);

        if (newProps.length == 0) {
            return;
        }

        List<Property> oldProps = replaceProperties(Arrays.asList(newProps));

        ctx.getStore(NAMESPACE).put(ctx.getUniqueId(), oldProps);
    }

    /** {@inheritDoc} */
    @Override
    public void afterEach(ExtensionContext ctx) {
        resetProperties(ctx);
    }

    /**
     * Replaces the system properties with the given properties.
     *
     * @return List of the previous property values.
     */
    private static List<Property> replaceProperties(List<WithSystemProperty> newProps) {
        // List of system properties to set when test is finished.
        var oldProps = new ArrayList<Property>(newProps.size());

        for (WithSystemProperty prop : newProps) {
            String oldVal = System.setProperty(prop.key(), prop.value());

            oldProps.add(new Property(prop.key(), oldVal));
        }

        return oldProps;
    }

    /**
     * Resets the system properties to the pre-test state.
     */
    private static void resetProperties(ExtensionContext ctx) {
        List<Property> oldProps = ctx.getStore(NAMESPACE).remove(ctx.getUniqueId(), List.class);

        if (oldProps == null) {
            return; // Nothing to do.
        }

        // Bring back the old properties in the reverse order
        Collections.reverse(oldProps);

        for (Property prop : oldProps) {
            if (prop.val == null) {
                System.clearProperty(prop.key);
            } else {
                System.setProperty(prop.key, prop.val);
            }
        }
    }

    /** Property. */
    private static class Property {
        /** Property key. */
        final String key;

        /** Property value. */
        @Nullable
        final String val;

        /**
         * Constructor.
         *
         * @param key Property key.
         * @param val Property value.
         */
        Property(String key, @Nullable String val) {
            this.key = key;
            this.val = val;
        }
    }
}
