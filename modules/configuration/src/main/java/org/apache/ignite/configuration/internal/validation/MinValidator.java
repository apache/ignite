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

import org.apache.ignite.configuration.validation.Min;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Validate that field value is not less than some minimal value.
 */
public class MinValidator implements Validator<Min, Number> {
    /** {@inheritDoc} */
    @Override public void validate(Min annotation, ValidationContext<Number> ctx) {
        if (ctx.getNewValue().longValue() < annotation.value()) {
            ctx.addIssue(new ValidationIssue(
                "Configuration value '" + ctx.currentKey() + "' must not be less than " + annotation.value()
            ));
        }
    }
}
