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

import java.util.Objects;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

public class ImmutableValidator implements Validator<Immutable, Object> {
    /** {@inheritDoc} */
    @Override public void validate(Immutable annotation, ValidationContext<Object> ctx) {
        Object oldValue = ctx.getOldValue();
        Object newValue = ctx.getNewValue();

        if (oldValue != null && !Objects.deepEquals(oldValue, newValue)) {
            ctx.addIssue(new ValidationIssue(
                "'" + ctx.currentKey() + "' configuration value is immutable and cannot be updated [curVal="
                    + oldValue + ", newVal=" + newValue + ']'
            ));
        }
    }
}
