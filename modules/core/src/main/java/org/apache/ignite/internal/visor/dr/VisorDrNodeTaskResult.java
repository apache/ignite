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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

public class VisorDrNodeTaskResult extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;
    /** Data center id. */
    private byte dataCenterId;
    /** Data node. */
    private boolean dataNode;
    /** Addresses. */
    private String addresses;
    /** Mode. */
    private String mode;
    /** Sender data centers. */
    private List<T2<Byte, List<String>>> senderDataCenters;
    /** Receiver address. */
    private String receiverAddress;
    /** Common config. */
    private List<T2<String, Object>> commonConfig;
    /** Sender config. */
    private List<T2<String, Object>> senderConfig;
    /** Receiver config. */
    private List<T2<String, Object>> receiverConfig;
    /** Sender metrics. */
    private List<T2<String, Object>> senderMetrics;
    /** Receiver metrics. */
    private List<T2<String, Object>> receiverMetrics;
    /** Response messages. */
    private List<String> responseMsgs = new ArrayList<>();

    /** */
    public void setDataCenterId(byte dataCenterId) {
        this.dataCenterId = dataCenterId;
    }

    /** */
    public byte getDataCenterId() {
        return dataCenterId;
    }

    /** */
    public void setDataNode(boolean dataNode) {
        this.dataNode = dataNode;
    }

    /** */
    public boolean getDataNode() {
        return dataNode;
    }

    /** */
    public void setAddresses(String addresses) {
        this.addresses = addresses;
    }

    /** */
    public String getAddresses() {
        return addresses;
    }

    /** */
    public void setMode(String mode) {
        this.mode = mode;
    }

    /** */
    public String getMode() {
        return mode;
    }

    /** */
    public void setSenderDataCenters(List<T2<Byte, List<String>>> senderDataCenters) {
        this.senderDataCenters = senderDataCenters;
    }

    /** */
    public List<T2<Byte, List<String>>> getSenderDataCenters() {
        return senderDataCenters;
    }

    /** */
    public void setReceiverAddress(String receiverAddress) {
        this.receiverAddress = receiverAddress;
    }

    /** */
    public String getReceiverAddress() {
        return receiverAddress;
    }

    /** */
    public void setSenderConfig(List<T2<String, Object>> config) {
        this.senderConfig = config;
    }

    /** */
    public List<T2<String, Object>> getSenderConfig() {
        return senderConfig;
    }

    /** */
    public void setReceiverConfig(List<T2<String, Object>> receiverConfig) {
        this.receiverConfig = receiverConfig;
    }

    /** */
    public List<T2<String, Object>> getReceiverConfig() {
        return receiverConfig;
    }

    /** */
    public void addResponseMessage(String msg) {
        this.responseMsgs.add(msg);
    }

    /** */
    public List<String> getResponseMsgs() {
        return responseMsgs;
    }

    /** */
    public void setSenderMetrics(List<T2<String, Object>> senderMetrics) {
        this.senderMetrics = senderMetrics;
    }

    /** */
    public List<T2<String, Object>> getSenderMetrics() {
        return senderMetrics;
    }

    /** */
    public void setReceiverMetrics(List<T2<String, Object>> receiverMetrics) {
        this.receiverMetrics = receiverMetrics;
    }

    /** */
    public List<T2<String, Object>> getReceiverMetrics() {
        return receiverMetrics;
    }

    /** */
    public void setCommonConfig(List<T2<String, Object>> commonConfig) {
        this.commonConfig = commonConfig;
    }

    /** */
    public List<T2<String, Object>> getCommonConfig() {
        return commonConfig;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        out.writeBoolean(dataNode);
        out.writeUTF(addresses);
        out.writeUTF(mode);
        U.writeCollection(out, senderDataCenters);
        out.writeObject(receiverAddress);
        U.writeCollection(out, commonConfig);
        U.writeCollection(out, senderConfig);
        U.writeCollection(out, receiverConfig);
        U.writeCollection(out, senderMetrics);
        U.writeCollection(out, receiverMetrics);
        U.writeCollection(out, responseMsgs);
    }

    /** {@inheritDoc} */
    @Override
    protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        dataNode = in.readBoolean();
        addresses = in.readUTF();
        mode = in.readUTF();
        senderDataCenters = U.readList(in);
        receiverAddress = (String)in.readObject();
        commonConfig = U.readList(in);
        senderConfig = U.readList(in);
        receiverConfig = U.readList(in);
        senderMetrics = U.readList(in);
        receiverMetrics = U.readList(in);
        responseMsgs = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrNodeTaskResult.class, this);
    }
}
