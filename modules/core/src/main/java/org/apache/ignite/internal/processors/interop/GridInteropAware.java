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

package org.apache.ignite.internal.processors.interop;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;

/**
 * Interface for interop-aware components.
 */
public interface GridInteropAware {
    /**
     * Sets configuration parameters.
     *
     * @param params Configuration parameters.
     */
    public void configure(Object... params);

    /**
     * Initializes interop-aware component.
     *
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    public void initialize(GridKernalContext ctx) throws IgniteCheckedException;

    /**
     * Destroys interop-aware component.
     *
     * @param ctx Context.
     * @throws IgniteCheckedException In case of error.
     */
    public void destroy(GridKernalContext ctx) throws IgniteCheckedException;
}
