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

package org.apache.ignite.compute.gridify;

import java.lang.annotation.Annotation;
import org.apache.ignite.IgniteCheckedException;

/**
 * This interface defines an interceptor apply for {@link Gridify} annotation. Interceptor
 * gets called in advise code to decide whether or not to grid-enable this method.
 * <p>
 * Interceptors can be used to provide fine-grain control on {@link Gridify} annotation
 * behavior. For example, an interceptor can be implemented to grid enable the method
 * only if CPU on the local node has been above 80% of utilization for the last 5 minutes.
 */
public interface GridifyInterceptor {
    /**
     * This method is called before actual grid-enabling happens.
     *
     * @param gridify Gridify annotation instance that caused the grid-enabling.
     * @param arg Gridify argument.
     * @return {@code True} if method should be grid-enabled, {@code false} otherwise.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public boolean isGridify(Annotation gridify, GridifyArgument arg) throws IgniteCheckedException;
}