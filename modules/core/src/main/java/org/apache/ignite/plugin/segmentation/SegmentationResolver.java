/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.plugin.segmentation;

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * This is interface for segmentation (a.k.a "split-brain" problem) resolvers.
 * <p>
 * Each segmentation resolver checks segment for validity, using its inner logic.
 * Typically, resolver should run light-weight single check (i.e. one IP address or
 * one shared folder). Compound segment checks may be performed using several
 * resolvers.
 * <p>
 * Note that Ignite support a logical segmentation and not limited to network
 * related segmentation only. For example, a particular segmentation resolver
 * can check for specific application or service present on the network and
 * mark the topology as segmented in case it is not available. In other words
 * you can equate the service outage with network outage via segmentation resolution
 * and employ the unified approach in dealing with these types of problems.
 * @see IgniteConfiguration#getSegmentationResolvers()
 * @see IgniteConfiguration#getSegmentationPolicy()
 * @see IgniteConfiguration#getSegmentCheckFrequency()
 * @see IgniteConfiguration#isAllSegmentationResolversPassRequired()
 * @see IgniteConfiguration#isWaitForSegmentOnStart()
 * @see SegmentationPolicy
 */
public interface SegmentationResolver extends Serializable {
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
     * and will try to reconnect via {@link SegmentationPolicy segmentation policy} set
     * in configuration.
     *
     * @return {@code True} if segment is correct, {@code false} otherwise.
     * @throws IgniteCheckedException If an error occurred.
     */
    public abstract boolean isValidSegment() throws IgniteCheckedException;
}