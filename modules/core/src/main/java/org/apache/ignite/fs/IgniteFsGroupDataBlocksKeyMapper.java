/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs;

import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * {@code GGFS} class providing ability to group file's data blocks together on one node.
 * All blocks within the same group are guaranteed to be cached together on the same node.
 * Group size parameter controls how many sequential blocks will be cached together on the same node.
 * <p>
 * For example, if block size is {@code 64kb} and group size is {@code 256}, then each group will contain
 * {@code 64kb * 256 = 16Mb}. Larger group sizes would reduce number of splits required to run map-reduce
 * tasks, but will increase inequality of data size being stored on different nodes.
 * <p>
 * Note that {@link #groupSize()} parameter must correlate to Hadoop split size parameter defined
 * in Hadoop via {@code mapred.max.split.size} property. Ideally you want all blocks accessed
 * within one split to be mapped to {@code 1} group, so they can be located on the same grid node.
 * For example, default Hadoop split size is {@code 64mb} and default {@code GGFS} block size
 * is {@code 64kb}. This means that to make sure that each split goes only through blocks on
 * the same node (without hopping between nodes over network), we have to make the {@link #groupSize()}
 * value be equal to {@code 64mb / 64kb = 1024}.
 * <p>
 * It is required for {@code GGFS} data cache to be configured with this mapper. Here is an
 * example of how it can be specified in XML configuration:
 * <pre name="code" class="xml">
 * &lt;bean id="cacheCfgBase" class="org.gridgain.grid.cache.GridCacheConfiguration" abstract="true"&gt;
 *     ...
 *     &lt;property name="affinityMapper"&gt;
 *         &lt;bean class="org.gridgain.grid.ggfs.GridGgfsGroupDataBlocksKeyMapper"&gt;
 *             &lt;!-- How many sequential blocks will be stored on the same node. --&gt;
 *             &lt;constructor-arg value="512"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 */
public class IgniteFsGroupDataBlocksKeyMapper extends GridCacheDefaultAffinityKeyMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Size of the group. */
    private final int grpSize;

    /***
     * Constructs affinity mapper to group several data blocks with the same key.
     *
     * @param grpSize Size of the group in blocks.
     */
    public IgniteFsGroupDataBlocksKeyMapper(int grpSize) {
        A.ensure(grpSize >= 1, "grpSize >= 1");

        this.grpSize = grpSize;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        if (key != null && GridGgfsBlockKey.class.equals(key.getClass())) {
            GridGgfsBlockKey blockKey = (GridGgfsBlockKey)key;

            if (blockKey.affinityKey() != null)
                return blockKey.affinityKey();

            long grpId = blockKey.getBlockId() / grpSize;

            return blockKey.getFileId().hashCode() + (int)(grpId ^ (grpId >>> 32));
        }

        return super.affinityKey(key);
    }

    /**
     * @return Size of the group.
     */
    public int groupSize() {
        return grpSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteFsGroupDataBlocksKeyMapper.class, this);
    }
}
