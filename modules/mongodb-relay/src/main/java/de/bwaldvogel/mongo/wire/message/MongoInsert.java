package de.bwaldvogel.mongo.wire.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import io.netty.channel.Channel;

public class MongoInsert extends ClientRequest {

    private final List<Document> documents;

    public MongoInsert(Channel channel, MessageHeader header, String fullCollectionName, List<Document> documents) {
        super(channel, header, fullCollectionName);
        this.documents = new ArrayList<>(documents);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("header: ").append(getHeader());
        sb.append(", collection: ").append(getFullCollectionName());
        sb.append(", #documents: ").append(documents.size());
        sb.append(")");
        return sb.toString();
    }

    public List<Document> getDocuments() {
        return Collections.unmodifiableList(documents);
    }

}
