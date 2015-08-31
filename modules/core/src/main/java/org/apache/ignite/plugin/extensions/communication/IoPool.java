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

package org.apache.ignite.plugin.extensions.communication;

import java.util.concurrent.Executor;
import org.apache.ignite.plugin.Extension;

/**
 * The interface of IO Messaging Pool Extension.
 */
public interface IoPool extends Extension {
    /**
     * Gets the numeric identifier of the pool. This identifier is to be taken from serialized
     * message and used to find the appropriate executor pool to process it.
     *
     * @return The id.
     */
    public byte id();

    /**
     * Gets the Executor for this Pool. Cannot be null.
     *
     * @return The executor.
     */
    public Executor executor();
}