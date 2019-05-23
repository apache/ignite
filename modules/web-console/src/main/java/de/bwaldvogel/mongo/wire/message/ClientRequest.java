package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.backend.Utils;
import io.netty.channel.Channel;

public abstract class ClientRequest implements Message {

    private final MessageHeader header;
    private final String fullCollectionName;
    private Channel channel;

    public ClientRequest(Channel channel, MessageHeader header, String fullCollectionName) {
        this.channel = channel;
        this.header = header;
        this.fullCollectionName = fullCollectionName;
    }

    public Channel getChannel() {
        return channel;
    }

    public MessageHeader getHeader() {
        return header;
    }

    @Override
    public String getDatabaseName() {
        return Utils.getDatabaseNameFromFullName(fullCollectionName);
    }

    public String getCollectionName() {
        return Utils.getCollectionNameFromFullName(fullCollectionName);
    }

    public String getFullCollectionName() {
        return fullCollectionName;
    }
}
