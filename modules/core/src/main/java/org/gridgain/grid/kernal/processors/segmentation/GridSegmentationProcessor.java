/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.segmentation;

import org.gridgain.grid.kernal.processors.*;

/**
 * Kernal processor responsible for checking network segmentation issues.
 * <p>
 * Segment checks are performed by segmentation resolvers
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 * @see org.gridgain.grid.IgniteConfiguration#getSegmentationResolvers()
 * @see org.gridgain.grid.IgniteConfiguration#getSegmentationPolicy()
 * @see org.gridgain.grid.IgniteConfiguration#getSegmentCheckFrequency()
 * @see org.gridgain.grid.IgniteConfiguration#isAllSegmentationResolversPassRequired()
 * @see org.gridgain.grid.IgniteConfiguration#isWaitForSegmentOnStart()
 */
public interface GridSegmentationProcessor extends GridProcessor {
    /**
     * Performs network segment check.
     * <p>
     * This method is called by discovery manager in the following cases:
     * <ol>
     *     <li>Before discovery SPI start.</li>
     *     <li>When other node leaves topology.</li>
     *     <li>When other node in topology fails.</li>
     *     <li>Periodically (see {@link org.gridgain.grid.IgniteConfiguration#getSegmentCheckFrequency()}).</li>
     * </ol>
     *
     * @return {@code True} if segment is correct.
     */
    public boolean isValidSegment();
}
