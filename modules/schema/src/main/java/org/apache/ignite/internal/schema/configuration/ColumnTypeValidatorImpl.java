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

package org.apache.ignite.internal.schema.configuration;

import java.util.Objects;
import org.apache.ignite.configuration.schemas.table.ColumnTypeValidator;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Column definition validator implementation validates column changes.
 */
public class ColumnTypeValidatorImpl implements Validator<ColumnTypeValidator, ColumnTypeView> {
    /** Static instance. */
    public static final ColumnTypeValidatorImpl INSTANCE = new ColumnTypeValidatorImpl();

    /** {@inheritDoc} */
    @Override public void validate(ColumnTypeValidator annotation, ValidationContext<ColumnTypeView> ctx) {
        ColumnTypeView newType = ctx.getNewValue();
        ColumnTypeView oldType = ctx.getOldValue();

        try {
            SchemaConfigurationConverter.convert(newType);
        } catch (IllegalArgumentException ex) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey() + ": " + ex.getMessage()));

            return;
        }

        if (oldType == null)
            return; // Nothing to do.

        if (!Objects.deepEquals(newType.type(), oldType.type()) ||
                newType.precision() != oldType.precision() ||
                newType.scale() != oldType.scale() ||
                newType.length() != oldType.length())
            ctx.addIssue(new ValidationIssue("Unsupported column type change: " + ctx.currentKey()));

    }

    /** Private constructor. */
    private ColumnTypeValidatorImpl() {
    }
}
