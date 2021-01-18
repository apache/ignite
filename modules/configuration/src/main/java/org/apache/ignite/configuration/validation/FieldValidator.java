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

package org.apache.ignite.configuration.validation;

import java.io.Serializable;
import org.apache.ignite.configuration.ConfigurationTree;

/**
 * Base class for field validator. Contains exception message.
 * @param <T> Field type.
 * @param <C> Root configuration type.
 */
public abstract class FieldValidator<T extends Serializable, C extends ConfigurationTree<?, ?>> {
    /** Validation error message. */
    protected final String message;

    /** Constructor. */
    protected FieldValidator(String message) {
        this.message = message;
    }

    /**
     * Validate field.
     *
     * @param value New value.
     * @param newRoot New configuration root.
     * @param oldRoot Old configuration root.
     * @throws ConfigurationValidationException If validation failed.
     */
    public abstract void validate(T value, C newRoot, C oldRoot) throws ConfigurationValidationException;
}
