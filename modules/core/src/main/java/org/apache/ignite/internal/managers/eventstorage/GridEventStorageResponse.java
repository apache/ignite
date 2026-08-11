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
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.UseBinaryMarshaller;
import org.apache.ignite.internal.util.ErrorMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/** Events collected for a {@link GridEventStorageRequest}, or the failure that prevented it. */
@UseBinaryMarshaller
public class GridEventStorageResponse implements Message {
    /** */
    @Marshalled("evtsBytes")
    Collection<Event> evts;

    /** */
    @Order(0)
    byte[] evtsBytes;

    /** */
    @Order(1)
    ErrorMessage errMsg;

    /** */
    public GridEventStorageResponse() {
        // No-op.
    }

    /**
     * @param evts Grid events.
     * @param ex Exception occurred during processing.
     */
    GridEventStorageResponse(Collection<Event> evts, @Nullable Throwable ex) {
        this.evts = evts;

        if (ex != null)
            errMsg = new ErrorMessage(ex);
    }

    /** @return Events. */
    @Nullable Collection<Event> events() {
        return evts != null ? Collections.unmodifiableCollection(evts) : null;
    }

    /** @return Exception. */
    @Nullable Throwable exception() {
        return ErrorMessage.error(errMsg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageResponse.class, this);
    }
}
