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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.marshaller.Marshaller;

import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

/**
 * Base class for all IGFS communication messages sent between nodes.
 */
public abstract class IgfsCommunicationMessage implements MarshallableMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param marsh Marshaller.
     * @throws IgniteCheckedException In case of error.
     */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @throws IgniteCheckedException In case of error.
     */
    @Override public void finishUnmarshal(Marshaller marsh, @Nullable ClassLoader ldr) throws IgniteCheckedException {
        // No-op.
    }

}
