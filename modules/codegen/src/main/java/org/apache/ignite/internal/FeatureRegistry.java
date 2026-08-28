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
 * Links the annotated class to the specified {@link IgniteFeature} registry. The registry
 * is used to resolve fully qualified names of features that introduced or deprecated fields
 * declared by the annotated class (see {@link Order#introducedBy()} and {@link Order#deprecatedBy()}).
 *
 * <p>If this annotation is absent, the Ignite Core feature registry is used.</p>
 *
 * @see Order
 * @see IgniteFeature
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface FeatureRegistry {
    /** @return Class of the associated feature registry. */
    Class<?> value();
}
