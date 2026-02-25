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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Event storage message.
 */
public class GridEventStorageMessage implements Message {
    /** */
    private Object resTopic;

    /** */
    @Order(0)
    byte[] resTopicBytes;

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
    @Order(value = 8, method = "loaderParticipants")
    Map<UUID, IgniteUuid> ldrParties;

    /** */
    public GridEventStorageMessage() {
        // No-op.
    }

    /**
     * @param resTopic Response topic,
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
        this.resTopic = resTopic;
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

        resTopic = null;
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
        return resTopic;
    }

    /**
     * @return Filter.
     */
    public IgnitePredicate<?> filter() {
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
    public IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Filter class name.
     */
    public String filterClassName() {
        return filterClsName;
    }

    /**
     * @return User version.
     */
    public String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    public @Nullable Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /**
     * @param ldrParties Node class loader participant map.
     */
    public void loaderParticipants(@Nullable Map<UUID, IgniteUuid> ldrParties) {
        this.ldrParties = ldrParties;
    }

    /**
     * @return Exception.
     */
    @Nullable Throwable exception() {
        return ErrorMessage.error(errMsg);
    }

    /**
     * @param marsh Marshaller.
     */
    public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        if (resTopic != null && resTopicBytes == null)
            resTopicBytes = U.marshal(marsh, resTopic);

        if (filter != null && filterBytes == null)
            filterBytes = U.marshal(marsh, filter);

        if (evts != null && evtsBytes == null)
            evtsBytes = U.marshal(marsh, evts);
    }

    /**
     * @param marsh Marshaller.
     * @param ldr Class loader.
     * @param filterClsLdr Class loader for filter.
     */
    public void finishUnmarshal(Marshaller marsh, ClassLoader ldr, ClassLoader filterClsLdr) throws IgniteCheckedException {
        if (resTopicBytes != null && resTopic == null) {
            resTopic = U.unmarshal(marsh, resTopicBytes, ldr);

            resTopicBytes = null;
        }

        if (filterBytes != null && filter == null && filterClsLdr != null) {
            filter = U.unmarshal(marsh, filterBytes, filterClsLdr);

            filterBytes = null;
        }

        if (evtsBytes != null && evts == null) {
            evts = U.unmarshal(marsh, evtsBytes, ldr);

            evtsBytes = null;
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 13;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageMessage.class, this);
    }
}
