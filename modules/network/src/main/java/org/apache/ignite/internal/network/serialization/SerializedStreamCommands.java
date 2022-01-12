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

package org.apache.ignite.internal.network.serialization;

/**
 * Lists commands used in the serialized stream.
 * Command IDs share space with IDs of {@link ClassDescriptor}s (most importantly, those that are defined in {@link BuiltinType}.
 */
public class SerializedStreamCommands {
    /**
     * Reference: an object that was already seen in the graph, so we relate to it by its ID instead of serializing it again.
     */
    public static final int REFERENCE = 44;

    private SerializedStreamCommands() {
    }
}
