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

import java.lang.annotation.Annotation;
import org.apache.ignite.configuration.RootKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Validation context for the validators.
 *
 * @param <VIEWT> Type of the subtree or the value that is being validated.
 * @see Validator#validate(Annotation, ValidationContext)
 */
public interface ValidationContext<VIEWT> {
    /**
     * Returns the key of validated node.
     *
     * @return String representation of currently validated value, i.e. {@code root.config.node}.
     */
    String currentKey();

    /**
     * Returns previous value of the configuration.
     *
     * @return Previous value of the configuration. Might be null for leaves only.
     */
    @Nullable VIEWT getOldValue();

    /**
     * Returns updated value of the configuration.
     *
     * @return Updated value of the configuration. Cannot be null.
     */
    @NotNull VIEWT getNewValue();

    /**
     * Returns previous value of the configuration root.
     *
     * @param rootKey Root key.
     * @param <ROOT>  Root view type derived from the root key.
     * @return Configuration root view before updates.
     */
    @Nullable <ROOT> ROOT getOldRoot(RootKey<?, ROOT> rootKey);

    /**
     * Returns updated value of the configuration root.
     *
     * @param rootKey Root key.
     * @param <ROOT>  Root view type derived from the root key.
     * @return Configuration root view after updates.
     */
    @Nullable <ROOT> ROOT getNewRoot(RootKey<?, ROOT> rootKey);

    /**
     * Signifies that there's something wrong. Values will be accumulated and passed to the user later.
     *
     * @param issue Validation issue object.
     * @see ConfigurationValidationException
     */
    void addIssue(ValidationIssue issue);
}
