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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * This annotation marks a configuration schema field as special (read-only) for getting the key associated with the configuration in the
 * named list. Can be used for nested configuration schema as well, but then {@link Name} should be used.
 * <pre><code>
 * {@literal @}Config
 *  public class DataRegionConfigurationSchema {
 *      {@literal @InjectedName}
 *       public String name;
 *
 *      {@literal @}Value
 *       public long size;
 * }
 *
 * {@literal @}Config
 *  public class DataStorageConfigurationSchema {
 *      {@literal @}Name("default")
 *      {@literal @}ConfigValue
 *       public DataRegionConfigurationSchema defaultRegion;
 *
 *      {@literal @}NamedConfigValue
 *       public DataRegionConfigurationSchema regions;
 * }
 * </code></pre>
 *
 * <p>NOTE: Field must be a {@link String} and the only one (with this annotation) in the schema, field name is used instead of
 * {@link NamedConfigValue#syntheticKeyName()}, it can be used in schemas with {@link Config} and {@link PolymorphicConfig}.
 *
 * @see Config
 * @see PolymorphicConfig
 * @see ConfigValue
 * @see NamedConfigValue
 * @see Name
 */
@Target(FIELD)
@Retention(RUNTIME)
@Documented
public @interface InjectedName {
}
