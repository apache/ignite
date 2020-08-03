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

package org.apache.ignite.springdata22.repository.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * The annotation can be used to pass Ignite specific parameters to a bound repository.
 *
 * @author Apache Ignite Team
 * @author Manuel Núñez (manuel.nunez@hawkore.com)
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface RepositoryConfig {
    /**
     * Cache name string.
     *
     * @return A name of a distributed Apache Ignite cache an annotated repository will be mapped to.
     */
    String cacheName() default "";

    /**
     * Ignite instance string. Default "igniteInstance".
     *
     * @return {@link Ignite} instance spring bean name
     */
    String igniteInstance() default "igniteInstance";

    /**
     * Ignite cfg string. Default "igniteCfg".
     *
     * @return {@link IgniteConfiguration} spring bean name
     */
    String igniteCfg() default "igniteCfg";

    /**
     * Ignite spring cfg path string. Default "igniteSpringCfgPath".
     *
     * @return A path to Ignite's Spring XML configuration spring bean name
     */
    String igniteSpringCfgPath() default "igniteSpringCfgPath";

    /**
     * Auto create cache. Default false to enforce control over cache creation and to avoid cache creation by mistake
     * <p>
     * Tells to Ignite Repository factory wether cache should be auto created if not exists.
     *
     * @return the boolean
     */
    boolean autoCreateCache() default false;
}
