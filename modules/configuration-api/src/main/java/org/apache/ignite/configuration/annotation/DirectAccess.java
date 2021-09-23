/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.configuration.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.DirectConfigurationProperty;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation that marks a configuration field or a whole configuration that can be accessed directly from the storage.
 * Such fields or configurations will be generated as a {@link DirectConfigurationProperty} in addition to regular
 * {@link ConfigurationProperty}.
 * <p>
 * Usage note: for implementation-specific reasons it is illegal to place this annotation on schema fields that are
 * either annotated with {@link ConfigValue} or {@link NamedConfigValue}. For creating direct nested configurations
 * this annotation should be placed on the corresponding {@link Config} classes.
 *
 * @see DirectConfigurationProperty DirectConfigurationProperty class for details about direct properties.
 */
@Target({TYPE, FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface DirectAccess {
}
