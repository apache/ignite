package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoDelete extends ClientRequest {

    private Document selector;
    private boolean singleRemove;

    public MongoDelete(Channel channel, MessageHeader header, String fullCollectionName, Document selector,
            boolean singleRemove) {
        super(channel, header, fullCollectionName);
        this.selector = selector;
        this.singleRemove = singleRemove;
    }

    public Document getSelector() {
        return selector;
    }

    public boolean isSingleRemove() {
        return singleRemove;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", selector: ").append(selector);
        sb.append(")");
        return sb.toString();
    }

}
