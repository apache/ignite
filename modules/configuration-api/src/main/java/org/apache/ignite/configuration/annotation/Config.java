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

package org.apache.ignite.configuration.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation, if applied to a class, marks it as a configuration schema.
 * Annotation processor generates several classes for each configuration schema:
 * <ul>
 * <li>Config - Represents configuration itself, provides API to init, change and view it. Extends {@code DynamicConfiguration}</li>
 * <li>Change - changes config tree</li>
 * <li>View - immutable object to view config tree</li>
 * </ul>
 *
 * <h1 class="header">Example</h1>
 * Here is how to create a root configuration schema:
 * <pre><code>
 * {@literal @}Config(value = "local", root = true)
 * public class LocalConfigurationSchema {
 *
 *      {@literal @}Value
 *      public String foo;
 *
 *      {@literal @}Value
 *      public boolean bar;
 *
 *      {@literal @}ConfigValue
 *      public SomeOtherConfiguration someOther;
 * }
 * </code></pre>
 */
@Target({ TYPE })
@Retention(RUNTIME)
@Documented
public @interface Config {
}
