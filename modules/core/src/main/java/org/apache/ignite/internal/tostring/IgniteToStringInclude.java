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

package org.apache.ignite.internal.tostring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.ignite.lang.IgniteSystemProperties;

/**
 * Class or field annotated with IgniteToStringInclude claims the element <b>should</b> be included
 * in {@code toString()} output.
 * This annotation is used to override the default exclusion policy.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface IgniteToStringInclude {
    /**
     * A flag indicating if sensitive information stored in the field or fields of the class.<br/>
     * Such information will be included to {@code toString()} output according to
     * {@link IgniteSystemProperties#IGNITE_SENSITIVE_DATA_LOGGING} policy.
     *
     * @return Attribute value.
     * @see SensitiveDataLoggingPolicy}.
     */
    boolean sensitive() default false;
}
