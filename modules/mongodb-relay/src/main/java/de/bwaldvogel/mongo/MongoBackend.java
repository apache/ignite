package de.bwaldvogel.mongo;

import java.time.Clock;
import java.util.Collection;
import java.util.List;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public interface MongoBackend {

    void handleClose(Channel channel);

    Document handleCommand(Channel channel, String database, String command, Document query);

    Iterable<Document> handleQuery(MongoQuery query);

    void handleInsert(MongoInsert insert);

    void handleDelete(MongoDelete delete);

    void handleUpdate(MongoUpdate update);

    void dropDatabase(String database);

    Collection<Document> getCurrentOperations(MongoQuery query);

    List<Integer> getVersion();

    void close();

    Clock getClock();

    void setClock(Clock clock);

}
