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

import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteUuid;

/**
 * The base class to block key that is used by the {@link IgfsGroupDataBlocksKeyMapper}
 */
@GridInternal
public interface IgfsBaseBlockKey {
    /**
     * @return Block ID.
     */
    public long getBlockId();

    /**
     * @return Hash is based on a file identifier (path, ID, etc).
     */
    public int getFileHash();

    /**
     * @return Block affinity key.
     */
    public IgniteUuid affinityKey();
}