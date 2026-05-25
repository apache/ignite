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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridTopicMessage;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Event storage message.
 */
public class GridEventStorageMessage implements MarshallableMessage {
    /** */
    @Order(0)
    GridTopicMessage resTopicMsg;

    /** */
    private IgnitePredicate<?> filter;

    /** */
    @Order(1)
    byte[] filterBytes;

    /** */
    private Collection<Event> evts;

    /** */
    @Order(2)
    byte[] evtsBytes;

    /** */
    @Order(3)
    ErrorMessage errMsg;

    /** */
    @Order(4)
    IgniteUuid clsLdrId;

    /** */
    @Order(5)
    DeploymentMode depMode;

    /** */
    @Order(6)
    String filterClsName;

    /** */
    @Order(7)
    String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @Order(8)
    Map<UUID, IgniteUuid> ldrParties;

    /** */
    public GridEventStorageMessage() {
        // No-op.
    }

    /**
     * @param resTopic Response topic.
     * @param filter Query filter.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridEventStorageMessage(
        Object resTopic,
        IgnitePredicate<?> filter,
        IgniteUuid clsLdrId,
        DeploymentMode depMode,
        String userVer,
        Map<UUID, IgniteUuid> ldrParties) {
        resTopicMsg = new GridTopicMessage(resTopic);
        this.filter = filter;
        filterClsName = filter.getClass().getName();
        this.depMode = depMode;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.ldrParties = ldrParties;

        evts = null;
        errMsg = null;
    }

    /**
     * @param evts Grid events.
     * @param ex Exception occurred during processing.
     */
    GridEventStorageMessage(Collection<Event> evts, Throwable ex) {
        this.evts = evts;

        if (ex != null)
            errMsg = new ErrorMessage(ex);

        resTopicMsg = null;
        filter = null;
        filterClsName = null;
        depMode = null;
        clsLdrId = null;
        userVer = null;
    }

    /**
     * @return Response topic.
     */
    Object responseTopic() {
        return GridTopicMessage.topic(resTopicMsg);
    }

    /**
     * @return Filter.
     */
    IgnitePredicate<?> filter() {
        return filter;
    }

    /**
     * @return Events.
     */
    @Nullable Collection<Event> events() {
        return evts != null ? Collections.unmodifiableCollection(evts) : null;
    }

    /**
     * @return the Class loader ID.
     */
    IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Filter class name.
     */
    String filterClassName() {
        return filterClsName;
    }

    /**
     * @return User version.
     */
    String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    @Nullable Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /**
     * @return Exception.
     */
    @Nullable Throwable exception() {
        return ErrorMessage.error(errMsg);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (filter != null)
            filterBytes = U.marshal(marsh, filter);

        if (evts != null)
            evtsBytes = U.marshal(marsh, evts);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader ldr) throws IgniteCheckedException {
        if (evtsBytes != null) {
            evts = U.unmarshal(marsh, evtsBytes, ldr);

            evtsBytes = null;
        }
    }

    /**
     * @param marsh Marshaller.
     * @param filterClsLdr Class loader for filter.
     */
    public void finishUnmarshalFilters(Marshaller marsh, ClassLoader filterClsLdr) throws IgniteCheckedException {
        if (filterBytes != null && filter == null) {
            filter = U.unmarshal(marsh, filterBytes, filterClsLdr);

            filterBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageMessage.class, this);
    }
}
