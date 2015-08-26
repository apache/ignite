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

import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.internal.processors.platform.callback.*;
import org.apache.ignite.internal.processors.platform.memory.*;

import java.util.*;

/**
 * Platform context. Acts as an entry point for platform operations.
 */
public interface PlatformContext {
    /**
     * Gets kernal context.
     *
     * @return Kernal context.
     */
    public GridKernalContext kernalContext();

    /**
     * Gets platform memory manager.
     *
     * @return Memory manager.
     */
    public PlatformMemoryManager memory();

    /**
     * Gets platform callback gateway.
     *
     * @return Callback gateway.
     */
    public PlatformCallbackGateway gateway();

    /**
     * Get memory reader.
     *
     * @param mem Memory.
     * @return Reader.
     */
    public PortableRawReaderEx reader(PlatformMemory mem);

    /**
     * Get memory reader.
     *
     * @param in Input.
     * @return Reader.
     */
    public PortableRawReaderEx reader(PlatformInputStream in);

    /**
     * Get memory writer.
     *
     * @param mem Memory.
     * @return Writer.
     */
    public PortableRawWriterEx writer(PlatformMemory mem);

    /**
     * Get memory writer.
     *
     * @param out Output.
     * @return Writer.
     */
    public PortableRawWriterEx writer(PlatformOutputStream out);

    /**
     * Sends node info to native platform, if necessary.
     *
     * @param node Node.
     */
    public void addNode(ClusterNode node);

    /**
     * Writes a node id to a stream and sends node info to native platform, if necessary.
     *
     * @param writer Writer.
     * @param node Node.
     */
    public void writeNode(PortableRawWriterEx writer, ClusterNode node);

    /**
     * Writes multiple node ids to a stream and sends node info to native platform, if necessary.
     *
     * @param writer Writer.
     * @param nodes Nodes.
     */
    public void writeNodes(PortableRawWriterEx writer, Collection<ClusterNode> nodes);

    /**
     * Process metadata from the platform.
     *
     * @param reader Reader.
     */
    public void processMetadata(PortableRawReaderEx reader);
}
