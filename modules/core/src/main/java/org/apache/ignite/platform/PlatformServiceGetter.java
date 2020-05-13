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

package org.apache.ignite.platform;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for setting mapping between java interface's method and getter of platform service property (mostly useful
 * for .NET services). Name mapping constructed by concatenating {@link PlatformServiceGetter#prefix}
 * with {@link PlatformServiceGetter#value}.
 * <p/>
 * For example, this annotated java inerface method:
 * <pre>
 * &#64;PlatformServiceGetter("SomeProperty")
 * SomeProperty getSomeProperty()
 * </pre>
 * will be mapped to {@code get_SomeProperty} method name and corresponds to the following .NET property:
 * <pre>
 * SomeProperty &#123; get; &#125;
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface PlatformServiceGetter {
    /**
     * Property name in corresponding platform service.
     */
    String value();

    /**
     * Prefix for platform getter.
     */
    String prefix() default "get_";
}
