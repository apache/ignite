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

package org.apache.ignite.internal.visor.dr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;

public class VisorDrCacheTaskArgs extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Cache filter all. */
    public static final int CACHE_FILTER_ALL = 0;
    /** Cache filter sending. */
    public static final int CACHE_FILTER_SENDING = 1;
    /** Cache filter receiving. */
    public static final int CACHE_FILTER_RECEIVING = 2;
    /** Cache filter paused. */
    public static final int CACHE_FILTER_PAUSED = 3;
    /** Cache filter error. */
    public static final int CACHE_FILTER_ERROR = 4;

    /** Sender group all. */
    public static final int SENDER_GROUP_ALL = 0;
    /** Sender group default. */
    public static final int SENDER_GROUP_DEFAULT = 1;
    /** Sender group none. */
    public static final int SENDER_GROUP_NONE = 2;
    /** Sender group named. */
    public static final int SENDER_GROUP_NAMED = 3;

    /** Action stop. */
    public static final int ACTION_STOP = 0;
    /** Action start. */
    public static final int ACTION_START = 1;
    /** Action full state transfer. */
    public static final int ACTION_FULL_STATE_TRANSFER = 2;
    /** Action none. */
    public static final int ACTION_NONE = 3;

    /** Regex. */
    private String regex;
    /** Config. */
    private boolean config;
    /** Metrics. */
    private boolean metrics;
    /** Filter. */
    private int filter;
    /** Sender group. */
    private int senderGroup;
    /** Sender group name. */
    private String senderGroupName;
    /** Action. */
    private int action;
    /** Remote data center id. */
    private byte remoteDataCenterId;

    /** */
    public String getRegex() {
        return regex;
    }

    /** */
    public boolean isConfig() {
        return config;
    }

    /** */
    public boolean isMetrics() {
        return metrics;
    }

    /** */
    public int getFilter() {
        return filter;
    }

    /** */
    public int getSenderGroup() {
        return senderGroup;
    }

    /** */
    public String getSenderGroupName() {
        return senderGroupName;
    }

    /** */
    public int getAction() {
        return action;
    }

    /** */
    public byte getRemoteDataCenterId() {
        return remoteDataCenterId;
    }

    /**
     * Default constructor.
     */
    public VisorDrCacheTaskArgs() {
        // No-op.
    }

    /** */
    public VisorDrCacheTaskArgs(
        String regex,
        boolean config,
        boolean metrics,
        int filter,
        int senderGroup,
        String senderGroupName,
        int action,
        byte remoteDataCenterId
    ) {
        this.regex = regex;
        this.config = config;
        this.metrics = metrics;
        this.filter = filter;
        this.senderGroup = senderGroup;
        this.senderGroupName = senderGroupName;
        this.action = action;
        this.remoteDataCenterId = remoteDataCenterId;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeUTF(regex);
        out.writeBoolean(config);
        out.writeBoolean(metrics);
        out.writeInt(filter);
        out.writeInt(senderGroup);
        out.writeObject(senderGroupName);
        out.writeInt(action);
        out.writeByte(remoteDataCenterId);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        regex = in.readUTF();
        config = in.readBoolean();
        metrics = in.readBoolean();
        filter = in.readInt();
        senderGroup = in.readInt();
        senderGroupName = (String)in.readObject();
        action = in.readInt();
        remoteDataCenterId = in.readByte();
    }
}
