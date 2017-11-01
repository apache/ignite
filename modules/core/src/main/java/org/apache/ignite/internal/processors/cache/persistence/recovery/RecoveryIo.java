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

package org.apache.ignite.internal.processors.cache.persistence.recovery;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 *
 */
public interface RecoveryIo {
    /**
     * @param constId ConsistentId.
     * @param msg Message.
     */
    public void send(String constId, Message msg) throws IgniteCheckedException;

    /**
     * @param handler Message handler.
     */
    public void receive(IgniteBiInClosure<String, Message> handler);

    /**
     * @param handler Node left handler.
     */
    public void onNodeLeft(IgniteInClosure<String> handler);

    /**
     * @param ver Affinity version.
     */
    public List<String> constIds(AffinityTopologyVersion ver);

    /**
     * @return Consistent id for local node.
     */
    public String localNodeConsistentId() throws IgniteCheckedException;
}
