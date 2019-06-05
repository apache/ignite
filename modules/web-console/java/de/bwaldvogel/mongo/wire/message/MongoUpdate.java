package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoUpdate extends ClientRequest {

    private Document selector;
    private Document update;
    private boolean upsert;
    private boolean multi;

    public MongoUpdate(Channel channel, MessageHeader header, String fullCollectionName, Document selector,
                       Document update, boolean upsert, boolean multi) {
        super(channel, header, fullCollectionName);
        this.selector = selector;
        this.update = update;
        this.upsert = upsert;
        this.multi = multi;
    }

    public boolean isUpsert() {
        return upsert;
    }

    public boolean isMulti() {
        return multi;
    }

    public Document getSelector() {
        return selector;
    }

    public Document getUpdate() {
        return update;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", selector: ").append(selector);
        sb.append(", update: ").append(update);
        sb.append(")");
        return sb.toString();
    }

}
