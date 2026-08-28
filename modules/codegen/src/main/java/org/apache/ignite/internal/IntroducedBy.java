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
 * Marks a newly introduced message field. Introducing a new field requires introducing a new
 * {@link IgniteFeature} to which this annotation must be linked.
 *
 * <p>A field annotated with this annotation is included in message serialization only when doing so does not break
 * backward compatibility during a Rolling Upgrade.</p>
 *
 * @see IgniteFeature
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.FIELD)
public @interface IntroducedBy {
    /** @return Name of the Ignite feature that introduced this field. */
    String value();

    /**
     * @return Class of the registry containing the Ignite feature with the specified name.
     * By default, the Ignite Core feature registry is used.
     */
    Class<?> registry() default Void.class;
}
