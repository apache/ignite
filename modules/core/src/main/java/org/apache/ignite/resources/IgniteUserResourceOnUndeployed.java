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

package org.apache.ignite.resources;

import java.lang.annotation.*;

/**
 * Annotates a special method inside injected user-defined resources {@link IgniteUserResource}. This
 * annotation is typically used to de-initialize user-defined resource. For example, the method with
 * this annotation can close database connection, or perform certain cleanup. Note that this method
 * will be called before any injected resources on this user-defined resource are cleaned up.
 * <p>
 * Here is how annotation would typically happen:
 * <pre name="code" class="java">
 * public class MyUserResource {
 *     ...
 *     &#64;GridLoggerResource
 *     private GridLogger log;
 *
 *     &#64;GridSpringApplicationContextResource
 *     private ApplicationContext springCtx;
 *     ...
 *     &#64;GridUserResourceOnUndeployed
 *     private void deploy() {
 *         log.info("Deploying resource: " + this);
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See also {@link IgniteUserResourceOnDeployed} for deployment callbacks.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IgniteUserResourceOnUndeployed {
    // No-op.
}
