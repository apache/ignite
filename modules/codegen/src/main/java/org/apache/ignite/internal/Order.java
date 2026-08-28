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

package org.apache.ignite.internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;

/**
 * The annotation specifies the position of a field in the serialized and deserialized byte sequence of a {@code Message} class.
 * <p>
 * The {@code value} indicates the index of the field in the serialization order.
 * Fields annotated with {@code @Order} are processed in ascending order of their index.
 * <p> By default, it is assumed that getters and setters are named as the annotated fields,
 * e.g. field 'val' should have getters and satters with name 'val' (according Ignite's to code-style).
 * <p> This annotation must be used on non-static fields, and access to those fields
 * should be performed strictly through corresponding getter and setter methods
 * following the naming convention: {@code fieldName()} for getter and {@code fieldName(Type)} for setter.
 */
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.FIELD)
public @interface Order {
    /** @return Order of the field. */
    int value();

    /**
     * Marks a newly introduced message field. Introducing a new field requires introducing a new
     * {@link IgniteFeature} to which this element must be linked. The feature is resolved in the registry named
     * by {@link FeatureRegistry} on the declaring class.
     *
     * <p>A field with this element set is included in message serialization only when doing so does not break
     * backward compatibility during a Rolling Upgrade.</p>
     *
     * @return Name of the Ignite feature that introduced this field, or an empty string if the field is not guarded.
     */
    String introducedBy() default "";

    /**
     * Marks a message field that is planned for removal in a future release. Removing the field requires introducing
     * a new {@link IgniteFeature} to which this element must be linked. The feature is resolved in the registry
     * named by {@link FeatureRegistry} on the declaring class.
     *
     * <p>A field with this element set is excluded from message serialization when doing so does not break
     * backward compatibility during a Rolling Upgrade.</p>
     *
     * @return Name of the Ignite feature that deprecated this field, or an empty string if the field is not guarded.
     */
    String deprecatedBy() default "";
}
