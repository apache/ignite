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

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.deployment.GridDeployment;

/**
 * Resource injector implementations contain logic and resources that
 * should be injected for selected target objects.
 */
interface GridResourceInjector {
    /**
     * Injects resource into field. Caches injected resource with the given key if needed.
     *
     * @param field Field to inject.
     * @param target Target object the field belongs to.
     * @param depCls Deployed class.
     * @param dep Deployment.
     * @throws IgniteCheckedException If injection failed.
     */
    public void inject(GridResourceField field, Object target, Class<?> depCls, GridDeployment dep) throws IgniteCheckedException;

    /**
     * Injects resource with a setter method. Caches injected resource with the given key if needed.
     *
     * @param mtd Setter method.
     * @param target Target object the field belongs to.
     * @param depCls Deployed class.
     * @param dep Deployment.
     * @throws IgniteCheckedException If injection failed.
     */
    public void inject(GridResourceMethod mtd, Object target, Class<?> depCls, GridDeployment dep) throws IgniteCheckedException;

    /**
     * Gracefully cleans all resources associated with deployment.
     *
     * @param dep Deployment to undeploy.
     */
    public void undeploy(GridDeployment dep);
}