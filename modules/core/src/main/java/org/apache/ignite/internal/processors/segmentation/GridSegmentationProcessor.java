/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.segmentation;

import org.apache.ignite.internal.processors.GridProcessor;

/**
 * Kernal processor responsible for checking network segmentation issues.
 * <p>
 * Segment checks are performed by segmentation resolvers
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentationResolvers()
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentationPolicy()
 * @see org.apache.ignite.configuration.IgniteConfiguration#getSegmentCheckFrequency()
 * @see org.apache.ignite.configuration.IgniteConfiguration#isAllSegmentationResolversPassRequired()
 * @see org.apache.ignite.configuration.IgniteConfiguration#isWaitForSegmentOnStart()
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
     *     <li>Periodically (see {@link org.apache.ignite.configuration.IgniteConfiguration#getSegmentCheckFrequency()}).</li>
     * </ol>
     *
     * @return {@code True} if segment is correct.
     */
    public boolean isValidSegment();
}