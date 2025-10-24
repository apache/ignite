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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * Message related to particular cache group.
 */
public abstract class GridCacheGroupIdMessage extends GridCacheMessage {
    /** Cache group ID. */
    @GridToStringInclude
    @Order(value = 3, method = "groupId")
    protected int grpId;

    /**
     * @return Cache group ID.
     */
    public int groupId() {
        return grpId;
    }

    /**
     * @param grpId Cache group ID.
     */
    public void groupId(int grpId) {
        this.grpId = grpId;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(MessageWriter writer) {
        // TODO: Safe to remove only after all inheritors have migrated to the new ser/der scheme (IGNITE-25490).
        if (!super.writeTo(writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 3:
                if (!writer.writeInt(grpId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(MessageReader reader) {
        // TODO: Safe to remove only after all inheritors have migrated to the new ser/der scheme (IGNITE-25490).
        if (!super.readFrom(reader))
            return false;

        switch (reader.state()) {
            case 3:
                grpId = reader.readInt();

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheGroupIdMessage.class, this);
    }
}
