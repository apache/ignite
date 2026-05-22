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

package org.apache.ignite.internal.managers.encryption;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/** */
public class NodeEncryptionKeys implements Message {
    /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node (in compatible format). */
    @Order(0)
    Map<Integer, byte[]> knownKeys;

    /**  New keys i.e. keys for a local statically configured caches. */
    @Order(1)
    Map<Integer, byte[]> newKeys;

    /** Master key digest. */
    @Order(2)
    byte[] masterKeyDigest;

    /** Known i.e. stored in {@code ReadWriteMetastorage} keys from node. */
    @Order(3)
    Map<Integer, List<GroupKeyEncrypted>> knownKeysWithIds;

    /** */
    public NodeEncryptionKeys() {}

    /** */
    NodeEncryptionKeys(
        HashMap<Integer, List<GroupKeyEncrypted>> knownKeysWithIds,
        Map<Integer, byte[]> newKeys,
        byte[] masterKeyDigest
    ) {
        this.newKeys = newKeys;
        this.masterKeyDigest = masterKeyDigest;

        if (F.isEmpty(knownKeysWithIds))
            return;

        // To be able to join the old cluster.
        knownKeys = U.newHashMap(knownKeysWithIds.size());

        for (Map.Entry<Integer, List<GroupKeyEncrypted>> entry : knownKeysWithIds.entrySet())
            knownKeys.put(entry.getKey(), entry.getValue().get(0).key());

        this.knownKeysWithIds = knownKeysWithIds;
    }
}
