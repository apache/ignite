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

/**
 * Interface for all configuration validators. Recommended to be a stateless class.
 *
 * <p>It is mandatory that all direct implementations of the interface explicitly specify types {@code A} and {@code VIEW}.
 *
 * @param <A>    Type of the annotation that puts current validator to the field.
 * @param <VIEWT> Upper bound for field types that can be validated with this validator.
 */
public interface Validator<A extends Annotation, VIEWT> {
    /**
     * Perform validation. All validation issues must be put into {@link ValidationContext#addIssue(ValidationIssue)}.
     *
     * @param annotation Specific annotation from currently validated value.
     * @param ctx        Validation context.
     */
    void validate(A annotation, ValidationContext<VIEWT> ctx);
}
