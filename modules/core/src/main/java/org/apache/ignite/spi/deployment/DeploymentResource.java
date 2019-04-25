/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.deployment;

/**
 * Generic abstraction over deployed resource containing resource's name, class and corresponding class loader.
 */
public interface DeploymentResource {
    /**
     * Gets resource name, either class name or alias name, such as alias
     * specified by {@link org.apache.ignite.compute.ComputeTaskName} annotation.
     *
     * @return Resource name.
     */
    public String getName();

    /**
     * Gets resource class.
     *
     * @return Resource class.
     */
    public Class<?> getResourceClass();

    /**
     * Gets resource class loader.
     *
     * @return Resource class loader.
     */
    public ClassLoader getClassLoader();
}