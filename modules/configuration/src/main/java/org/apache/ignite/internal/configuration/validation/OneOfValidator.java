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

package org.apache.ignite.internal.configuration.validation;

import java.util.Arrays;
import org.apache.ignite.configuration.validation.OneOf;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * {@link Validator} implementation for the {@link OneOf} annotation.
 */
public class OneOfValidator implements Validator<OneOf, String> {
    /** {@inheritDoc} */
    @Override
    public void validate(OneOf annotation, ValidationContext<String> ctx) {
        String value = ctx.getNewValue();

        boolean caseSensitive = annotation.caseSensitive();

        for (String exp : annotation.value()) {
            if (caseSensitive ? exp.equals(value) : exp.equalsIgnoreCase(value)) {
                return;
            }
        }

        String message = "'" + ctx.currentKey() + "' configuration value must be one of "
                + Arrays.toString(annotation.value()) + (caseSensitive ? " (case sensitive)" : " (case insensitive)");

        ctx.addIssue(new ValidationIssue(message));
    }
}
