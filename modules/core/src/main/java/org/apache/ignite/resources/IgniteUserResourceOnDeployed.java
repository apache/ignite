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
 * Annotates a special methods inside injected user-defined resource {@link IgniteUserResource}.
 * It can be used in any injected resource for grid tasks and grid jobs. Typically the method with this
 * annotation will be used for initialization of the injectable resource such as opening database connection,
 * network connection or reading configuration settings. Note that this method is called after the resource
 * itself has been injected with all its resources, if any.
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
 *     &#64;GridUserResourceOnDeployed
 *     private void deploy() {
 *         log.info("Deploying resource: " + this);
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See also {@link IgniteUserResourceOnUndeployed} for undeployment callbacks.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface IgniteUserResourceOnDeployed {
    // No-op.
}
