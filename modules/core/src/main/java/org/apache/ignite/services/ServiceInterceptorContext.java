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

package org.apache.ignite.services;

import org.apache.ignite.lang.IgniteExperimental;
import org.jetbrains.annotations.Nullable;

/**
 * Mutble service method invocation context.
 */
@IgniteExperimental
public interface ServiceInterceptorContext extends ServiceCallContext {
    /**
     * Get invocation method name.
     *
     * @return Method name.
     */
    public String method();

    /**
     * Get method arguments.
     *
     * @return Method arguments.
     */
    public @Nullable Object[] arguments();

    /**
     * Set the string attribute.
     *
     * @param name Attribute name.
     * @param val Attrbiute value.
     */
    public void attribute(String name, String val);

    /**
     * Set the binary attribute.
     *
     * @param name Attribute name.
     * @param val Binary attrbiute value.
     */
    public void binaryAttribute(String name, byte[] val);
}
