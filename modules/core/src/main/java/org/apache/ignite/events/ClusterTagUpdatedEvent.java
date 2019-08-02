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
package org.apache.ignite.events;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;

/**
 * Event type indicating that cluster tag has been updated.
 *
 * @see EventType#EVT_CLUSTER_TAG_UPDATED
 */
public class ClusterTagUpdatedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** ID of cluster. */
    private final UUID clusterId;

    /** Previous value of tag. */
    private final String prevTag;

    /** New value of tag. */
    private final String newTag;

    /**
     * @param node Node on which the event was fired.
     * @param msg Optional event message.
     * @param clusterId ID of cluster which tag was updated.
     * @param prevTag Previous cluster tag replaced during update.
     * @param newTag New cluster tag.
     */
    public ClusterTagUpdatedEvent(ClusterNode node, String msg, UUID clusterId,
        String prevTag, String newTag) {
        super(node, msg, EventType.EVT_CLUSTER_TAG_UPDATED);
        this.clusterId = clusterId;
        this.prevTag = prevTag;
        this.newTag = newTag;
    }

    /**
     * Cluster ID which tag was updated.
     *
     * @return UUID of cluster.
     */
    public UUID clusterId() {
        return clusterId;
    }

    /**
     * Value of cluster tag before update request that triggered this event.
     *
     * @return Previous value of tag.
     */
    public String previousTag() {
        return prevTag;
    }

    /**
     * Value of cluster tag after update request that triggered this event.
     *
     * @return New value of tag.
     */
    public String newTag() {
        return newTag;
    }
}
