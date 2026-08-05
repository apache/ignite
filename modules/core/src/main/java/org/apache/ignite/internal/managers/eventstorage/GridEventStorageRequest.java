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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.DeferredUnmarshalMessage;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

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

    /** */
    @Order(2)
    IgniteUuid clsLdrId;

    /** */
    @Order(3)
    DeploymentMode depMode;

    /** */
    @Order(4)
    String filterClsName;

    /** */
    @Order(5)
    String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(6)
    Map<UUID, IgniteUuid> ldrParties;

    /** */
    public GridEventStorageRequest() {
        // No-op.
    }

    /**
     * @param resTopicId Id of the node waiting for the response.
     * @param filter Query filter.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridEventStorageRequest(
        IgniteUuid resTopicId,
        IgnitePredicate<?> filter,
        IgniteUuid clsLdrId,
        DeploymentMode depMode,
        String userVer,
        Map<UUID, IgniteUuid> ldrParties) {
        this.resTopicId = resTopicId;
        this.filter = filter;
        this.clsLdrId = clsLdrId;
        this.depMode = depMode;
        this.userVer = userVer;
        this.ldrParties = ldrParties;

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

    /** @return Class loader ID. */
    IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /** @return Deployment mode. */
    DeploymentMode deploymentMode() {
        return depMode;
    }

    /** @return Filter class name. */
    String filterClassName() {
        return filterClsName;
    }

    /** @return User version. */
    String userVersion() {
        return userVer;
    }

    /** @return Node class loader participant map. */
    @Nullable Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageRequest.class, this);
    }
}
