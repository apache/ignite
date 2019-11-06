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

package org.apache.ignite.igfs;

import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.igfs.IgfsBaseBlockKey;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * {@code IGFS} class providing ability to group file's data blocks together on one node.
 * All blocks within the same group are guaranteed to be cached together on the same node.
 * Group size parameter controls how many sequential blocks will be cached together on the same node.
 * <p>
 * For example, if block size is {@code 64kb} and group size is {@code 256}, then each group will contain
 * {@code 64kb * 256 = 16Mb}. Larger group sizes would reduce number of splits required to run map-reduce
 * tasks, but will increase inequality of data size being stored on different nodes.
 * <p>
 * Note that {@link #getGroupSize()} parameter must correlate to Hadoop split size parameter defined
 * in Hadoop via {@code mapred.max.split.size} property. Ideally you want all blocks accessed
 * within one split to be mapped to {@code 1} group, so they can be located on the same grid node.
 * For example, default Hadoop split size is {@code 64mb} and default {@code IGFS} block size
 * is {@code 64kb}. This means that to make sure that each split goes only through blocks on
 * the same node (without hopping between nodes over network), we have to make the {@link #getGroupSize()}
 * value be equal to {@code 64mb / 64kb = 1024}.
 * <p>
 * It is required for {@code IGFS} data cache to be configured with this mapper. Here is an
 * example of how it can be specified in XML configuration:
 * <pre name="code" class="xml">
 * &lt;bean id="cacheCfgBase" class="org.apache.ignite.cache.CacheConfiguration" abstract="true"&gt;
 *     ...
 *     &lt;property name="affinityMapper"&gt;
 *         &lt;bean class="org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper"&gt;
 *             &lt;!-- How many sequential blocks will be stored on the same node. --&gt;
 *             &lt;property name="groupSize" value="512"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 */
public class IgfsGroupDataBlocksKeyMapper extends GridCacheDefaultAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default group size.*/
    public static final int DFLT_GRP_SIZE = 1024;

    /** Size of the group. */
    private int grpSize = DFLT_GRP_SIZE;

    /**
     * Default constructor.
     */
    public IgfsGroupDataBlocksKeyMapper() {
        // No-op.
    }

    /***
     * Constructs affinity mapper to group several data blocks with the same key.
     *
     * @param grpSize Size of the group in blocks.
     */
    public IgfsGroupDataBlocksKeyMapper(int grpSize) {
        A.ensure(grpSize >= 1, "grpSize >= 1");

        this.grpSize = grpSize;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        if (key instanceof IgfsBaseBlockKey) {
            IgfsBaseBlockKey blockKey = (IgfsBaseBlockKey)key;

            IgniteUuid affKey = blockKey.affinityKey();

            if (affKey != null)
                return affKey;

            long grpId = blockKey.blockId() / grpSize;

            return blockKey.fileHash() + (int)(grpId ^ (grpId >>> 32));
        }

        return super.affinityKey(key);
    }

    /**
     * Get group size.
     * <p>
     * Group size defines how many sequential file blocks will reside on the same node. This parameter
     * must correlate to Hadoop split size parameter defined in Hadoop via {@code mapred.max.split.size}
     * property. Ideally you want all blocks accessed within one split to be mapped to {@code 1} group,
     * so they can be located on the same grid node. For example, default Hadoop split size is {@code 64mb}
     * and default {@code IGFS} block size is {@code 64kb}. This means that to make sure that each split
     * goes only through blocks on the same node (without hopping between nodes over network), we have to
     * make the group size be equal to {@code 64mb / 64kb = 1024}.
     * <p>
     * Defaults to {@link #DFLT_GRP_SIZE}.
     *
     * @return Group size.
     */
    public int getGroupSize() {
        return grpSize;
    }

    /**
     * Set group size. See {@link #getGroupSize()} for more information.
     *
     * @param grpSize Group size.
     * @return {@code this} for chaining.
     */
    public IgfsGroupDataBlocksKeyMapper setGroupSize(int grpSize) {
        this.grpSize = grpSize;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsGroupDataBlocksKeyMapper.class, this);
    }
}
