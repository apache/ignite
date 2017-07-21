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
import org.apache.ignite.internal.IgnitionEx;

/**
 * Interface was introduced to avoid compile-time dependency on spring framework. Spring resource context
 * provides optional spring resource injectors, it can be passed to factory method
 * starting Ignite {@link IgnitionEx#start(GridSpringResourceContext)}.
 */
public interface GridSpringResourceContext {
    /**
     * @return Spring bean injector.
     */
    public GridResourceInjector springBeanInjector();

    /**
     * @return Spring context injector.
     */
    public GridResourceInjector springContextInjector();

    /**
     * Return original object if AOP used with proxy objects.
     *
     * @param target Target object.
     * @return Original object wrapped by proxy.
     * @throws IgniteCheckedException If unwrap failed.
     */
    public Object unwrapTarget(Object target) throws IgniteCheckedException;
}
