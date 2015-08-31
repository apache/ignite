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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.GridProcessor;

/**
 * Platform processor.
 */
public interface PlatformProcessor extends GridProcessor {
    /**
     * Get owning Ignite instance.
     *
     * @return Ignite instance.
     */
    public Ignite ignite();

    /**
     * Get environment pointer associated with this processor.
     *
     * @return Environment pointer.
     */
    public long environmentPointer();

    /**
     * Gets platform context.
     *
     * @return Platform context.
     */
    public PlatformContext context();

    /**
     * Await until platform processor is safe to use.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void awaitStart() throws IgniteCheckedException;

    /**
     * Get platform extensions. Override this method to provide any additional targets and operations you need.
     *
     * @return Platform extensions.
     */
    PlatformTarget extensions();
}