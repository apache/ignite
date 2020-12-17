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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * This annotation marks configuration schema field as a configuration tree node.
 * <pre name="code" class="java">
 * {@literal @}Config
 * public class FooConfigurationSchema {
 *
 *      {@literal @}ConfigValue
 *      private SomeOtherConfiguration someOther;
 *
 * }
 * </pre>
 */
@Target({ FIELD })
@Retention(SOURCE)
@Documented
public @interface ConfigValue {
    /**
     * @return The name of the configuration.
     */
    String value() default "";
}
