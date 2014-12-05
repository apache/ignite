/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.segmentation;

import org.gridgain.grid.*;

import java.io.*;

/**
 * This is interface for segmentation (a.k.a "split-brain" problem) resolvers.
 * <p>
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 * <p>
 * Note that GridGain support a logical segmentation and not limited to network
 * related segmentation only. For example, a particular segmentation resolver
 * can check for specific application or service present on the network and
 * mark the topology as segmented in case it is not available. In other words
 * you can equate the service outage with network outage via segmentation resolution
 * and employ the unified approach in dealing with these types of problems.
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentationResolvers()
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentationPolicy()
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentCheckFrequency()
 * @see org.apache.ignite.configuration.IgniteConfiguration#isAllSegmentationResolversPassRequired()
 * @see org.apache.ignite.configuration.IgniteConfiguration#isWaitForSegmentOnStart()
 * @see GridSegmentationPolicy
 */
public interface GridSegmentationResolver extends Serializable {
    /**
     * Checks whether segment is valid.
     * <p>
     * When segmentation happens every node ends up in either one of two segments:
     * <ul>
     *     <li>Correct segment</li>
     *     <li>Invalid segment</li>
     * </ul>
     * Nodes in correct segment will continue operate as if nodes in the invalid segment
     * simply left the topology (i.e. the topology just got "smaller"). Nodes in the
     * invalid segment will realized that were "left out or disconnected" from the correct segment
     * and will try to reconnect via {@link GridSegmentationPolicy segmentation policy} set
     * in configuration.
     *
     * @return {@code True} if segment is correct, {@code false} otherwise.
     * @throws GridException If an error occurred.
     */
    public abstract boolean isValidSegment() throws GridException;
}
