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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class VisorDrCacheTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;
    /** Data center id. */
    private byte dataCenterId;
    /** Cache names. */
    private List<String> cacheNames;
    /** Sender config. */
    private Map<String, List<T2<String, Object>>> senderConfig;
    /** Receiver config. */
    private Map<String, List<T2<String, Object>>> receiverConfig;
    /** Sender metrics. */
    private Map<String, List<T2<String, Object>>> senderMetrics;
    /** Receiver metrics. */
    private Map<String, List<T2<String, Object>>> receiverMetrics;
    /** Result messages. */
    private List<String> resultMessages = new ArrayList<>();

    /** */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /** */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /** */
    public void setCacheNames(List<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /** */
    public List<String> getCacheNames() {
        return cacheNames;
    }

    /** */
    public void setSenderConfig(Map<String, List<T2<String, Object>>> senderConfig) {
        this.senderConfig = senderConfig;
    }

    /** */
    public Map<String, List<T2<String, Object>>> getSenderConfig() {
        return senderConfig;
    }

    /** */
    public void setReceiverConfig(Map<String, List<T2<String, Object>>> receiverConfig) {
        this.receiverConfig = receiverConfig;
    }

    /** */
    public Map<String, List<T2<String, Object>>> getReceiverConfig() {
        return receiverConfig;
    }

    /** */
    public void setSenderMetrics(Map<String, List<T2<String, Object>>> senderMetrics) {
        this.senderMetrics = senderMetrics;
    }

    /** */
    public Map<String, List<T2<String, Object>>> getSenderMetrics() {
        return senderMetrics;
    }

    /** */
    public void setReceiverMetrics(Map<String, List<T2<String, Object>>> receiverMetrics) {
        this.receiverMetrics = receiverMetrics;
    }

    /** */
    public Map<String, List<T2<String, Object>>> getReceiverMetrics() {
        return receiverMetrics;
    }

    /** */
    public List<String> getResultMessages() {
        return resultMessages;
    }

    /** */
    public void addResultMessage(String resultMessage) {
        resultMessages.add(resultMessage);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        U.writeCollection(out, resultMessages);
        U.writeCollection(out, cacheNames);
        U.writeMap(out, senderConfig);
        U.writeMap(out, receiverConfig);
        U.writeMap(out, senderMetrics);
        U.writeMap(out, receiverMetrics);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        resultMessages = U.readList(in);
        cacheNames = U.readList(in);
        senderConfig = U.readMap(in);
        receiverConfig = U.readMap(in);
        senderMetrics = U.readMap(in);
        receiverMetrics = U.readMap(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrCacheTaskResult.class, this);
    }
}
