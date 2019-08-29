package de.bwaldvogel.mongo.backend;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.BsonConstants;
import de.bwaldvogel.mongo.wire.MongoWireProtocolHandler;
import de.bwaldvogel.mongo.wire.message.Message;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public abstract class AbstractMongoBackend implements MongoBackend {

    private static final Logger log = LoggerFactory.getLogger(AbstractMongoBackend.class);

    private final Map<String, MongoDatabase> databases = new TreeMap<>();

    private final List<Integer> version = Arrays.asList(3, 0, 0);

    private Clock clock = Clock.systemDefaultZone();

    private int maxWireVersion = 2;
    private int minWireVersion = 0;

    private MongoDatabase resolveDatabase(Message message) {
        return resolveDatabase(message.getDatabaseName());
    }

    protected synchronized MongoDatabase resolveDatabase(String database) {
        MongoDatabase db = databases.get(database);
        if (db == null) {
            db = openOrCreateDatabase(database);
            log.info("created database {}", db.getDatabaseName());
            databases.put(database, db);
        }
        return db;
    }

    private Document getLog(String argument) {
        log.debug("getLog: {}", argument);
        Document response = new Document();
        switch (argument) {
            case "*":
                response.put("names", Collections.singletonList("startupWarnings"));
                Utils.markOkay(response);
                break;
            case "startupWarnings":
                response.put("totalLinesWritten", Integer.valueOf(0));
                response.put("log", new ArrayList<String>());
                Utils.markOkay(response);
                break;
            default:
                throw new MongoSilentServerException("no RamLog named: " + argument);
        }
        return response;
    }

    private Document handleAdminCommand(String command, Document query) {

        if (command.equalsIgnoreCase("listdatabases")) {
            Document response = new Document();
            List<Document> dbs = new ArrayList<>();
            for (MongoDatabase db : databases.values()) {
                Document dbObj = new Document("name", db.getDatabaseName());
                dbObj.put("empty", Boolean.valueOf(db.isEmpty()));
                dbs.add(dbObj);
            }
            response.put("databases", dbs);
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("find")) {
            String collectionName = (String) query.get(command);
            if (collectionName.equals("$cmd.sys.inprog")) {
                return Utils.cursorResponse(collectionName, new Document("inprog", Collections.emptyList()));
            } else {
                throw new NoSuchCommandException(new Document(command, collectionName).toString());
            }
        } else if (command.equalsIgnoreCase("replSetGetStatus")) {
            throw new MongoServerError(76, "NoReplicationEnabled", "not running with --replSet");
        } else if (command.equalsIgnoreCase("getLog")) {
            final Object argument = query.get(command);
            return getLog(argument == null ? null : argument.toString());
        } else if (command.equalsIgnoreCase("renameCollection")) {
            return handleRenameCollection(command, query);
        } else if (command.equalsIgnoreCase("getLastError")) {
            Document response = new Document();
            log.debug("getLastError on admin database");
            Utils.markOkay(response);
            return response;
        } else {
            throw new NoSuchCommandException(command);
        }
    }

    private Document handleRenameCollection(String command, Document query) {
        final String oldNamespace = query.get(command).toString();
        final String newNamespace = query.get("to").toString();
        boolean dropTarget = Utils.isTrue(query.get("dropTarget"));
        Document response = new Document();

        if (!oldNamespace.equals(newNamespace)) {
            MongoCollection<?> oldCollection = resolveCollection(oldNamespace);
            if (oldCollection == null) {
                throw new MongoServerException("source namespace does not exist");
            }

            String newDatabaseName = Utils.getDatabaseNameFromFullName(newNamespace);
            String newCollectionName = Utils.getCollectionNameFromFullName(newNamespace);
            MongoDatabase oldDatabase = resolveDatabase(oldCollection.getDatabaseName());
            MongoDatabase newDatabase = resolveDatabase(newDatabaseName);
            MongoCollection<?> newCollection = newDatabase.resolveCollection(newCollectionName, false);
            if (newCollection != null) {
                if (dropTarget) {
                    newDatabase.dropCollection(newCollectionName);
                } else {
                    throw new MongoServerError(48, "NamespaceExists", "target namespace exists");
                }
            }

            newDatabase.moveCollection(oldDatabase, oldCollection, newCollectionName);
        }
        Utils.markOkay(response);
        return response;
    }

    private MongoCollection<?> resolveCollection(final String namespace) {
        final String databaseName = Utils.getDatabaseNameFromFullName(namespace);
        final String collectionName = Utils.getCollectionNameFromFullName(namespace);

        MongoDatabase database = databases.get(databaseName);
        if (database == null) {
            return null;
        }

        MongoCollection<?> collection = database.resolveCollection(collectionName, false);
        if (collection == null) {
            return null;
        }

        return collection;
    }

    protected abstract MongoDatabase openOrCreateDatabase(String databaseName);

    @Override
    public Document handleCommand(Channel channel, String databaseName, String command, Document query) {
        if (command.equalsIgnoreCase("whatsmyuri")) {
            Document response = new Document();
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
            response.put("you", remoteAddress.getAddress().getHostAddress() + ":" + remoteAddress.getPort());
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("ismaster")) {
            Document response = new Document("ismaster", Boolean.TRUE);
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            response.put("maxWriteBatchSize", Integer.valueOf(MongoWireProtocolHandler.MAX_WRITE_BATCH_SIZE));
            response.put("maxMessageSizeBytes", Integer.valueOf(MongoWireProtocolHandler.MAX_MESSAGE_SIZE_BYTES));
            response.put("maxWireVersion", Integer.valueOf(maxWireVersion));
            response.put("minWireVersion", Integer.valueOf(minWireVersion));
            response.put("localTime", Instant.now(clock));
            Utils.markOkay(response);
            return response;
        } else if (command.equalsIgnoreCase("buildinfo")) {
            Document response = new Document("version", Utils.join(version, "."));
            response.put("versionArray", version);
            response.put("maxBsonObjectSize", Integer.valueOf(BsonConstants.MAX_BSON_OBJECT_SIZE));
            Utils.markOkay(response);
            return response;
        }

        if (databaseName.equals("admin")) {
            return handleAdminCommand(command, query);
        } else {
            MongoDatabase db = resolveDatabase(databaseName);
            return db.handleCommand(channel, command, query);
        }
    }

    @Override
    public Collection<Document> getCurrentOperations(MongoQuery query) {
        // TODO
        return Collections.emptyList();
    }

    @Override
    public Iterable<Document> handleQuery(MongoQuery query) {
        MongoDatabase db = resolveDatabase(query);
        return db.handleQuery(query);
    }

    @Override
    public void handleInsert(MongoInsert insert) {
        MongoDatabase db = resolveDatabase(insert);
        db.handleInsert(insert);
    }

    @Override
    public void handleDelete(MongoDelete delete) {
        MongoDatabase db = resolveDatabase(delete);
        db.handleDelete(delete);
    }

    @Override
    public void handleUpdate(MongoUpdate update) {
        MongoDatabase db = resolveDatabase(update);
        db.handleUpdate(update);
    }

    @Override
    public void dropDatabase(String databaseName) {
        MongoDatabase removedDatabase = databases.remove(databaseName);
        removedDatabase.drop();
    }

    @Override
    public void handleClose(Channel channel) {
        for (MongoDatabase db : databases.values()) {
            db.handleClose(channel);
        }
    }

    @Override
    public List<Integer> getVersion() {
        return version;
    }

    public void setVersion(int major, int minor, int patch) {
        version.set(0, major);
        version.set(1, minor);
        version.set(2, patch);
    }

    public void setWireVersion(int maxWireVersion, int minWireVersion) {
        this.maxWireVersion = maxWireVersion;
        this.minWireVersion = minWireVersion;
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public void setClock(Clock clock) {
        this.clock = clock;
    }

}
