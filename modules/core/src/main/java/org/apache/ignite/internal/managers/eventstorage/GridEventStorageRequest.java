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

package org.apache.ignite.internal.managers.eventstorage;

import org.apache.ignite.internal.DeferredUnmarshalMessage;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.GridTopic.TOPIC_EVENT;

/** Remote event query. The filter is a user class, hence the deferred unmarshalling. */
@UseBinaryMarshaller
public class GridEventStorageRequest implements DeferredUnmarshalMessage {
    /** */
    @Order(0)
    IgniteUuid resTopicId;

    /** */
    @Marshalled("filterBytes")
    IgnitePredicate<?> filter;

    /** */
    @Order(1)
    byte[] filterBytes;

    /** Deployment of the filter classes. */
    @Order(2)
    GridDeploymentInfoMessage depInfo;

    /** */
    @Order(3)
    String filterClsName;

    /** */
    public GridEventStorageRequest() {
        // No-op.
    }

    /**
     * @param resTopicId Id of the node waiting for the response.
     * @param filter Query filter.
     * @param depInfo Deployment of the filter classes.
     */
    GridEventStorageRequest(IgniteUuid resTopicId, IgnitePredicate<?> filter, GridDeploymentInfo depInfo) {
        this.resTopicId = resTopicId;
        this.filter = filter;
        this.depInfo = new GridDeploymentInfoMessage(depInfo);

        filterClsName = filter.getClass().getName();
    }

    /** @return Topic to answer to. */
    Object responseTopic() {
        return TOPIC_EVENT.topic(resTopicId);
    }

    /** @return Filter. */
    IgnitePredicate<?> filter() {
        return filter;
    }

    /** @return Deployment of the filter classes. */
    GridDeploymentInfo deploymentInfo() {
        return depInfo;
    }

    /** @return Filter class name. */
    String filterClassName() {
        return filterClsName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageRequest.class, this);
    }
}
