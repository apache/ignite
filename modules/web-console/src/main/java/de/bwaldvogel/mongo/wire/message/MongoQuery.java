package de.bwaldvogel.mongo.wire.message;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoQuery extends ClientRequest {

    private final Document query;
    private final Document returnFieldSelector;
    private int numberToSkip;
    private int numberToReturn;

    public MongoQuery(Channel channel, MessageHeader header, String fullCollectionName, int numberToSkip,
            int numberToReturn, Document query, Document returnFieldSelector) {
        super(channel, header, fullCollectionName);
        this.numberToSkip = numberToSkip;
        this.numberToReturn = numberToReturn;
        this.query = query;
        this.returnFieldSelector = returnFieldSelector;
    }

    public int getNumberToSkip() {
        return numberToSkip;
    }

    public int getNumberToReturn() {
        return numberToReturn;
    }

    public Document getQuery() {
        return query;
    }

    public Document getReturnFieldSelector() {
        return returnFieldSelector;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", query: ").append(query);
        sb.append(", returnFieldSelector: ").append(returnFieldSelector);
        sb.append(")");
        return sb.toString();
    }

}
