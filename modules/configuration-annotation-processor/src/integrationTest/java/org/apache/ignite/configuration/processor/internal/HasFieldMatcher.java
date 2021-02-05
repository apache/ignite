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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import spoon.reflect.reference.CtFieldReference;

/**
 * Hamcrest matcher that tests class for field existence.
 */
public class HasFieldMatcher extends BaseMatcher<ParsedClass> {
    /** Name of the expected field. */
    private String fieldName;

    /** Type of the expected field. */
    private String fieldType;

    /**
     * Constructor.
     * @param fieldName Name of the expected field.
     * @param fieldType Type of the expected field.
     */
    private HasFieldMatcher(String fieldName, String fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    /**
     * Create matcher for fields with types.
     * @param arguments Array of field names and field types, paired.
     * @return Matcher.
     */
    public static BaseMatcher<ParsedClass> hasFields(String... arguments) {
        if (arguments.length % 2 != 0)
            throw new RuntimeException("Number of field names should be equal to number of field types");

        List<HasFieldMatcher> matcherList = new ArrayList<>();

        for (int i = 0; i < arguments.length; i += 2)
            matcherList.add(new HasFieldMatcher(arguments[i], arguments[i + 1]));

        return new BaseMatcher<ParsedClass>() {
            /** Currently used matcher. */
            int currentMatcher = 0;

            /** {@inheritDoc} */
            @Override public void describeTo(Description description) {
                matcherList.get(currentMatcher).describeTo(description);
            }

            /** {@inheritDoc} */
            @Override public void describeMismatch(Object item, Description description) {
                matcherList.get(currentMatcher).describeMismatch(item, description);
            }

            /** {@inheritDoc} */
            @Override public boolean matches(Object o) {
                for (int i = 0; i < matcherList.size(); i++) {
                    currentMatcher = i;
                    if (!matcherList.get(i).matches(o))
                        return false;
                }

                return true;
            }
        };
    }

    /** {@inheritDoc} */
    @Override public boolean matches(Object o) {
        if (!(o instanceof ParsedClass))
            return false;

        ParsedClass cls = (ParsedClass) o;

        final Map<String, CtFieldReference<?>> fields = cls.getFields();
        final CtFieldReference<?> field = fields.get(fieldName);

        if (field == null)
            return false;

        return field.getType().getQualifiedName().equals(fieldType);
    }

    /** {@inheritDoc} */
    @Override public void describeTo(Description description) {
        description.appendText(String.format("has field \"%s\" with type \"%s\"", fieldName, fieldType));
    }

    /** {@inheritDoc} */
    @Override public void describeMismatch(Object item, Description description) {
        if (!(item instanceof ParsedClass)) {
            description.appendText("is not ParsedClass instance");
            return;
        }

        ParsedClass cls = (ParsedClass) item;

        final Map<String, CtFieldReference<?>> fields = cls.getFields();
        final CtFieldReference<?> field = fields.get(fieldName);

        if (field == null) {
            description.appendText("doesn't have field \"" + fieldName + "\"");
            return;
        }

        final String actualFieldType = field.getType().getQualifiedName();
        if (!actualFieldType.equals(fieldType))
            description.appendText(String.format("\"%s\" has incorrect type \"%s\"", fieldName, actualFieldType));
    }
}
