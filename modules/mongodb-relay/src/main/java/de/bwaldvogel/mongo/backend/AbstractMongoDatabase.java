package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.Constants.ID_FIELD;
import static de.bwaldvogel.mongo.backend.Constants.PRIMARY_KEY_INDEX_NAME;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.IndexNotFoundException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.MongoSilentServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;

public abstract class AbstractMongoDatabase<P> implements MongoDatabase {

    private static final String NAMESPACES_COLLECTION_NAME = "system.namespaces";

    private static final String INDEXES_COLLECTION_NAME = "system.indexes";

    protected static final Logger log = LoggerFactory.getLogger(AbstractMongoDatabase.class);

    protected final String databaseName;
    protected final MongoBackend backend;

    protected final Map<String, MongoCollection<P>> collections = new ConcurrentHashMap<>();

    protected final AtomicReference<MongoCollection<P>> indexes = new AtomicReference<>();

    protected final Map<Channel, List<Document>> lastResults = new ConcurrentHashMap<>();

    protected MongoCollection<P> namespaces;

    protected AbstractMongoDatabase(String databaseName, MongoBackend backend) {
        this.databaseName = databaseName;
        this.backend = backend;
    }

    protected void initializeNamespacesAndIndexes() {
        this.namespaces = openOrCreateCollection(NAMESPACES_COLLECTION_NAME, "name");
        this.collections.put(namespaces.getCollectionName(), namespaces);

        if (!namespaces.isEmpty()) {
            for (String name : listCollectionNamespaces()) {
            	if(isSystemCollection(name)) {
            		continue;
            	}
                log.debug("opening {}", name);                
                String collectionName = extractCollectionNameFromNamespace(name);
                MongoCollection<P> collection = openOrCreateCollection(collectionName, ID_FIELD);
                collections.put(collectionName, collection);
                log.debug("opened collection '{}'", collectionName);
            }

            MongoCollection<P> indexCollection = getOrCreateIndexesCollection();           
            for (Document indexDescription : indexCollection.queryAll()) {
                openOrCreateIndex(indexDescription);
            }
        }
    }

    @Override
    public final String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + getDatabaseName() + ")";
    }

    private Document commandDropDatabase() {
        backend.dropDatabase(getDatabaseName());
        Document response = new Document("dropped", getDatabaseName());
        Utils.markOkay(response);
        return response;
    }

    @Override
    public Document handleCommand(Channel channel, String command, Document query) {

        // getlasterror must not clear the last error
        if (command.equalsIgnoreCase("getlasterror")) {
            return commandGetLastError(channel, command, query);
        } else if (command.equalsIgnoreCase("reseterror")) {
            return commandResetError(channel);
        }

        clearLastStatus(channel);

        if (command.equalsIgnoreCase("find")) {
            return commandFind(command, query);
        } else if (command.equalsIgnoreCase("insert")) {
            return commandInsert(channel, command, query);
        } else if (command.equalsIgnoreCase("update")) {
            return commandUpdate(channel, command, query);
        } else if (command.equalsIgnoreCase("delete")) {
            return commandDelete(channel, command, query);
        } else if (command.equalsIgnoreCase("create")) {
            return commandCreate(command, query);
        } else if (command.equalsIgnoreCase("createIndexes")) {
            return commandCreateIndexes(query);
        } else if (command.equalsIgnoreCase("count")) {
            return commandCount(command, query);
        } else if (command.equalsIgnoreCase("aggregate")) {
            return commandAggregate(command, query);
        } else if (command.equalsIgnoreCase("distinct")) {
            MongoCollection<P> collection = resolveCollection(command, query, true);
            return collection.handleDistinct(query);
        } else if (command.equalsIgnoreCase("drop")) {
            return commandDrop(query);
        } else if (command.equalsIgnoreCase("dropDatabase")) {
            return commandDropDatabase();
        } else if (command.equalsIgnoreCase("dropIndexes")) {
            return commandDropIndexes(query);
        } else if (command.equalsIgnoreCase("dbstats")) {
            return commandDatabaseStats();
        } else if (command.equalsIgnoreCase("collstats")) {
            MongoCollection<P> collection = resolveCollection(command, query, false);
            if (collection == null) {
                Document emptyStats = new Document()
                    .append("count", 0)
                    .append("size", 0);
                Utils.markOkay(emptyStats);
                return emptyStats;
            } else {
                return collection.getStats();
            }
        } else if (command.equalsIgnoreCase("validate")) {
            MongoCollection<P> collection = resolveCollection(command, query, false);
            if (collection == null) {
                throw new MongoServerError(26, "NamespaceNotFound", "ns not found");
            }
            return collection.validate();
        } else if (command.equalsIgnoreCase("findAndModify")) {
            String collectionName = query.get(command).toString();
            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            return collection.findAndModify(query);
        } else if (command.equalsIgnoreCase("listCollections")) {
            return listCollections();
        } else if (command.equalsIgnoreCase("listIndexes")) {
            String collectionName = query.get(command).toString();
            return listIndexes(collectionName);
         //add@byron
        }else if (command.equalsIgnoreCase("usersInfo")) {
        	 Document selector = new Document("_id", query.get("usersInfo"));
        	 MongoCollection<P> collection = resolveOrCreateCollection("usersInfo");
        	 for (Document document : collection.handleQuery(selector, 0, 1, null)) {
                 return document;
             }
        	 Utils.markOkay(selector);
             return selector;
        }
        else if (command.equalsIgnoreCase("createUser")) {       
            Document response = new Document("_id", query.get("createUser"));   
            query.append("_id", query.get("createUser"));
            query.append("name", query.get("createUser"));
            query.remove("createUser");
            MongoCollection<P> collection = resolveOrCreateCollection("usersInfo");
            collection.addDocument(query);
            Utils.markOkay(response);
            return response;
       
        } 
		//end@
		else {
            log.error("unknown query: {}", query);
        }
        throw new NoSuchCommandException(command);
    }

    private Document listCollections() {
        List<Document> firstBatch = new ArrayList<>();
        for (String namespace : listCollectionNamespaces()) {
            if (namespace.endsWith(INDEXES_COLLECTION_NAME)) {
                continue;
            }
            Document collectionDescription = new Document();
            Document collectionOptions = new Document();
            String collectionName = extractCollectionNameFromNamespace(namespace);
            collectionDescription.put("name", collectionName);
            collectionDescription.put("options", collectionOptions);
            collectionDescription.put("info", new Document("readOnly", false));
            collectionDescription.put("type", "collection");
            collectionDescription.put("idIndex", getPrimaryKeyIndexDescription(namespace));
            firstBatch.add(collectionDescription);
        }

        return Utils.cursorResponse(getDatabaseName() + ".$cmd.listCollections", firstBatch);
    }

    @VisibleForExternalBackends
    protected static Document getPrimaryKeyIndexDescription(String namespace) {
        return new Document("key", new Document(ID_FIELD, 1))
            .append("name", Constants.PRIMARY_KEY_INDEX_NAME)
            .append("ns", namespace)
            .append("v", 2);
    }

    private Iterable<String> listCollectionNamespaces() {
        return namespaces.queryAllAsStream()
            .map(document -> document.get("name").toString())
            ::iterator;
    }

    private Document listIndexes(String collectionName) {
        Iterable<Document> indexes = Optional.ofNullable(resolveCollection(INDEXES_COLLECTION_NAME, false))
            .map(collection -> collection.handleQuery(new Document("ns", getDatabaseName() + "." + collectionName)))
            .orElse(Collections.emptyList());
        return Utils.cursorResponse(getDatabaseName() + ".$cmd.listIndexes", indexes);
    }

    private synchronized MongoCollection<P> resolveOrCreateCollection(String collectionName) {
        final MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            return collection;
        } else {
            return createCollection(collectionName);
        }
    }

    private Document commandFind(String command, Document query) {

        final List<Document> documents = new ArrayList<>();
        String collectionName = (String) query.get(command);
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            int numberToSkip = ((Number) query.getOrDefault("skip", 0)).intValue();
            int numberToReturn = ((Number) query.getOrDefault("limit", 0)).intValue();
            Document projection = (Document) query.get("projection");

            Document querySelector = new Document();
            querySelector.put("$query", query.getOrDefault("filter", new Document()));
            querySelector.put("$orderby", query.get("sort"));

            for (Document document : collection.handleQuery(querySelector, numberToSkip, numberToReturn, projection)) {
                documents.add(document);
            }
        }

        return Utils.cursorResponse(getDatabaseName() + "." + collectionName, documents);
    }

    private Document commandInsert(Channel channel, String command, Document query) {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> documents = (List<Document>) query.get("documents");

        List<Document> writeErrors = new ArrayList<>();
        int n = 0;
        List<Document> opt = new ArrayList<>(documents.size());
       
        for (Document document : documents) {
            try {
                insertDocuments(channel, collectionName, Collections.singletonList(document));
                opt.add(new Document(ID_FIELD,document.get(ID_FIELD)));
                n++;
            } catch (MongoServerError e) {
                Document error = new Document();
                error.put("index", Integer.valueOf(n));
                error.put("errmsg", e.getMessageWithoutErrorCode());
                error.put("code", Integer.valueOf(e.getCode()));
                error.putIfNotNull("codeName", e.getCodeName());
                writeErrors.add(error);
            }
        }
        Document result = new Document();
        result.put("n", Integer.valueOf(n));
        if (!writeErrors.isEmpty()) {
            result.put("writeErrors", writeErrors);
        }
        result.put("ops",opt);
        // odd by true: also mark error as okay
        Utils.markOkay(result);
        return result;
    }

    private Document commandUpdate(Channel channel, String command, Document query) {
        clearLastStatus(channel);
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> updates = (List<Document>) query.get("updates");
        int nMatched = 0;
        int nModified = 0;
        Collection<Document> upserts = new ArrayList<>();

        List<Document> writeErrors = new ArrayList<>();

        Document response = new Document();
        for (int i = 0; i < updates.size(); i++) {
            Document updateObj = updates.get(i);
            Document selector = (Document) updateObj.get("q");
            Document update = (Document) updateObj.get("u");
            ArrayFilters arrayFilters = ArrayFilters.parse(updateObj, update);
            boolean multi = Utils.isTrue(updateObj.get("multi"));
            boolean upsert = Utils.isTrue(updateObj.get("upsert"));
            final Document result;
            try {
                result = updateDocuments(collectionName, selector, update, arrayFilters, multi, upsert);
            } catch (MongoServerException e) {
                writeErrors.add(toWriteError(i, e));
                continue;
            }
            if (result.containsKey("upserted")) {
                final Object id = result.get("upserted");
                final Document upserted = new Document("index", i);
                upserted.put(ID_FIELD, id);
                upserts.add(upserted);
            }
            nMatched += ((Integer) result.get("n")).intValue();
            nModified += ((Integer) result.get("nModified")).intValue();
        }

        response.put("n", nMatched + upserts.size());
        response.put("nModified", nModified);
        if (!upserts.isEmpty()) {
            response.put("upserted", upserts);
        }
        if (!writeErrors.isEmpty()) {
            response.put("writeErrors", writeErrors);
        }
        response.put("ops",upserts);
       
        Utils.markOkay(response);
        putLastResult(channel, response);
        return response;
    }

    private Document commandDelete(Channel channel, String command, Document query) {
        String collectionName = query.get(command).toString();
        boolean isOrdered = Utils.isTrue(query.get("ordered"));
        log.trace("ordered: {}", isOrdered);

        @SuppressWarnings("unchecked")
        List<Document> deletes = (List<Document>) query.get("deletes");
        int n = 0;
        for (Document delete : deletes) {
            final Document selector = (Document) delete.get("q");
            final int limit = ((Number) delete.get("limit")).intValue();
            Document result = deleteDocuments(channel, collectionName, selector, limit);
            Integer resultNumber = (Integer) result.get("n");
            n += resultNumber.intValue();
        }

        Document response = new Document("n", Integer.valueOf(n));
        Utils.markOkay(response);
        return response;
    }

    private Document commandCreate(String command, Document query) {
        String collectionName = query.get(command).toString();
        boolean isCapped = Utils.isTrue(query.get("capped"));
        if (isCapped) {
            throw new MongoServerException("Creating capped collections is not yet implemented");
        }

        Object autoIndexId = query.get("autoIndexId");
        if (autoIndexId != null && !Utils.isTrue(autoIndexId)) {
            throw new MongoServerException("Disabling autoIndexId is not yet implemented");
        }

        createCollectionOrThrowIfExists(collectionName);

        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    @Override
    public MongoCollection<P> createCollectionOrThrowIfExists(String collectionName) {
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection != null) {
            throw new MongoServerError(48, "NamespaceExists",
                "a collection '" + getDatabaseName() + "." + collectionName + "' already exists");
        }

        return createCollection(collectionName);
    }

    private Document commandCreateIndexes(Document query) {
        int indexesBefore = countIndexes();

        @SuppressWarnings("unchecked")
        final Collection<Document> indexDescriptions = (Collection<Document>) query.get("indexes");
        for (Document indexDescription : indexDescriptions) {
        	//add@byron
        	if (!indexDescription.containsKey("ns")) {
                indexDescription.put("ns", this.getDatabaseName()+"."+query.getOrDefault("createIndexes",query.get("name")));
            }
            addIndex(indexDescription);
        }

        int indexesAfter = countIndexes();

        Document response = new Document();
        response.put("numIndexesBefore", Integer.valueOf(indexesBefore));
        response.put("numIndexesAfter", Integer.valueOf(indexesAfter));
        Utils.markOkay(response);
        return response;
    }

    private Document commandDropIndexes(Document query) {
        String collectionName = (String) query.get("dropIndexes");
        MongoCollection<P> collection = resolveCollection(collectionName, true);
        Object indexKeys0 =  query.get("index");
        Document indexQuery = null;
        if(indexKeys0 instanceof String) {
        	indexQuery = new Document("name", indexKeys0);        	
        }
        else {
        	indexQuery = new Document("key", indexKeys0);
        }
        Document indexKeys = (Document)indexQuery;
        
        MongoCollection<P> indexCollection = indexes.get();
        Document indexToDrop = CollectionUtils.getSingleElement(indexCollection.handleQuery(indexQuery),
            () -> new IndexNotFoundException(indexKeys));
        int numDeleted = dropIndex(collection, indexToDrop);
        //-Assert.equals(numDeleted, 1, () -> "Expected one deleted document");
        Document response = new Document();
        Utils.markOkay(response);
        return response;
    }

    private int dropIndex(MongoCollection<P> collection, Document indexDescription) {
        String indexName = (String) indexDescription.get("name");
        dropIndex(collection, indexName);
        return indexes.get().deleteDocuments(indexDescription, -1);
    }

    protected void dropIndex(MongoCollection<P> collection, String indexName) {
        collection.dropIndex(indexName);
    }

    private int countIndexes() {
        final MongoCollection<P> indexesCollection;
        synchronized (indexes) {
            indexesCollection = indexes.get();
        }
        if (indexesCollection == null) {
            return 0;
        } else {
            return indexesCollection.count();
        }
    }

    private Collection<MongoCollection<P>> collections() {
        return collections.values().stream()
            .filter(collection -> !isSystemCollection(collection.getCollectionName()))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private Document commandDatabaseStats() {
        Document response = new Document("db", getDatabaseName());
        response.put("collections", Integer.valueOf(collections().size()));

        long storageSize = getStorageSize();
        long fileSize = getFileSize();
        long indexSize = 0;
        int objects = 0;
        double dataSize = 0;
        double averageObjectSize = 0;

        for (MongoCollection<P> collection : collections()) {
            Document stats = collection.getStats();
            objects += ((Number) stats.get("count")).intValue();
            dataSize += ((Number) stats.get("size")).doubleValue();

            Document indexSizes = (Document) stats.get("indexSize");
            for (String indexName : indexSizes.keySet()) {
                indexSize += ((Number) indexSizes.get(indexName)).longValue();
            }

        }
        if (objects > 0) {
            averageObjectSize = dataSize / ((double) objects);
        }
        response.put("objects", Integer.valueOf(objects));
        response.put("avgObjSize", Double.valueOf(averageObjectSize));
        if (dataSize == 0.0) {
            response.put("dataSize", Integer.valueOf(0));
        } else {
            response.put("dataSize", Double.valueOf(dataSize));
        }
        response.put("storageSize", Long.valueOf(storageSize));
        response.put("numExtents", Integer.valueOf(0));
        response.put("indexes", Integer.valueOf(countIndexes()));
        response.put("indexSize", Long.valueOf(indexSize));
        response.put("fileSize", Long.valueOf(fileSize));
        response.put("nsSizeMB", Integer.valueOf(0));
        Utils.markOkay(response);
        return response;
    }

    protected abstract long getFileSize();

    protected abstract long getStorageSize();

    private Document commandDrop(Document query) {
        String collectionName = query.get("drop").toString();

        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            throw new MongoSilentServerException("ns not found");
        }

        int numIndexes = collection.getNumIndexes();
        dropCollection(collectionName);
        Document response = new Document();
        response.put("nIndexesWas", Integer.valueOf(numIndexes));
        response.put("ns", collection.getFullName());
        Utils.markOkay(response);
        return response;
    }

    private Document commandGetLastError(Channel channel, String command, Document query) {
        Iterator<String> it = query.keySet().iterator();
        String cmd = it.next();
        Assert.equals(cmd, command);
        if (it.hasNext()) {
            String subCommand = it.next();
            switch (subCommand) {
                case "w":
                    // ignore
                    break;
                case "fsync":
                    // ignore
                    break;
                default:
                    throw new MongoServerException("unknown subcommand: " + subCommand);
            }
        }

        List<Document> results = lastResults.get(channel);

        Document result;
        if (results != null && !results.isEmpty()) {
            result = results.get(results.size() - 1);
            if (result == null) {
                result = new Document();
            }
        } else {
            result = new Document();
            result.put("err", null);
            result.put("n", 0);
        }
        if (result.containsKey("writeErrors")) {
            @SuppressWarnings("unchecked")
            List<Document> writeErrors = (List<Document>) result.get("writeErrors");
            if (writeErrors.size() == 1) {
                result.putAll(CollectionUtils.getSingleElement(writeErrors));
                result.remove("writeErrors");
            }
        }
        Utils.markOkay(result);
        return result;
    }

    private Document commandResetError(Channel channel) {
        List<Document> results = lastResults.get(channel);
        if (results != null) {
            results.clear();
        }
        Document result = new Document();
        Utils.markOkay(result);
        return result;
    }

    private Document commandCount(String command, Document query) {
        MongoCollection<P> collection = resolveCollection(command, query, false);
        Document response = new Document();
        if (collection == null) {
            response.put("n", Integer.valueOf(0));
        } else {
            Document queryObject = (Document) query.get("query");
            int limit = getOptionalNumber(query, "limit", -1);
            int skip = getOptionalNumber(query, "skip", 0);
            response.put("n", Integer.valueOf(collection.count(queryObject, skip, limit)));
        }
        Utils.markOkay(response);
        return response;
    }

    private Document commandAggregate(String command, Document query) {
        String collectionName = query.get(command).toString();
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        Object pipeline = query.get("pipeline");
        Aggregation aggregation = Aggregation.fromPipeline(pipeline, this, collection);
        aggregation.validate(query);
        List<Document> aggregationResult = aggregation.computeResult();
        return Utils.cursorResponse(getDatabaseName() + "." + collectionName, aggregationResult);
    }

    private int getOptionalNumber(Document query, String fieldName, int defaultValue) {
        Number limitNumber = (Number) query.get(fieldName);
        return limitNumber != null ? limitNumber.intValue() : defaultValue;
    }

    @Override
    public Iterable<Document> handleQuery(MongoQuery query) {
        clearLastStatus(query.getChannel());
        String collectionName = query.getCollectionName();
        MongoCollection<P> collection = resolveCollection(collectionName, false);
        if (collection == null) {
            return Collections.emptyList();
        }
        int numSkip = query.getNumberToSkip();
        int numReturn = query.getNumberToReturn();
        Document fieldSelector = query.getReturnFieldSelector();
        return collection.handleQuery(query.getQuery(), numSkip, numReturn, fieldSelector);
    }

    @Override
    public void handleClose(Channel channel) {
        lastResults.remove(channel);
    }

    private synchronized void clearLastStatus(Channel channel) {
        List<Document> results = lastResults.computeIfAbsent(channel, k -> new LimitedList<>(10));
        results.add(null);
    }

    @Override
    public void handleInsert(MongoInsert insert) {
        Channel channel = insert.getChannel();
        String collectionName = insert.getCollectionName();
        List<Document> documents = insert.getDocuments();

        if (collectionName.equals(INDEXES_COLLECTION_NAME)) {
            for (Document indexDescription : documents) {
                addIndex(indexDescription);
            }
        } else {
            try {
                insertDocuments(channel, collectionName, documents);
            } catch (MongoServerException e) {
                log.error("failed to insert {}", insert, e);
            }
        }
    }

    private MongoCollection<P> resolveCollection(String command, Document query, boolean throwIfNotFound) {
        String collectionName = query.get(command).toString();
        return resolveCollection(collectionName, throwIfNotFound);
    }

    @Override
    public synchronized MongoCollection<P> resolveCollection(String collectionName, boolean throwIfNotFound) {
        checkCollectionName(collectionName);
        MongoCollection<P> collection = collections.get(collectionName);
        if (collection == null && throwIfNotFound) {
            throw new MongoServerException("Collection [" + getDatabaseName() + "." + collectionName + "] not found.");
        }
        return collection;
    }

    private void checkCollectionName(String collectionName) {
        if (collectionName.length() > Constants.MAX_NS_LENGTH) {
            throw new MongoServerError(10080, "ns name too long, max size is " + Constants.MAX_NS_LENGTH);
        }

        if (collectionName.isEmpty()) {
            throw new MongoServerError(16256, "Invalid ns [" + collectionName + "]");
        }
    }

    @Override
    public boolean isEmpty() {
        return collections.isEmpty();
    }

    private void addNamespace(MongoCollection<P> collection) {
        collections.put(collection.getCollectionName(), collection);
        if (!isSystemCollection(collection.getCollectionName())) {
        	try {
        		namespaces.addDocument(new Document("name", collection.getFullName()));
        	}catch(MongoServerException e) {
        		 log.error("failed to add namespace {}", collection.getFullName(), e);
        	}
        }
    }

    @Override
    public void handleDelete(MongoDelete delete) {
        Channel channel = delete.getChannel();
        String collectionName = delete.getCollectionName();
        Document selector = delete.getSelector();
        int limit = delete.isSingleRemove() ? 1 : Integer.MAX_VALUE;

        try {
            deleteDocuments(channel, collectionName, selector, limit);
        } catch (MongoServerException e) {
            log.error("failed to delete {}", delete, e);
        }
    }

    @Override
    public void handleUpdate(MongoUpdate updateCommand) {
        Channel channel = updateCommand.getChannel();
        String collectionName = updateCommand.getCollectionName();
        Document selector = updateCommand.getSelector();
        Document update = updateCommand.getUpdate();
        boolean multi = updateCommand.isMulti();
        boolean upsert = updateCommand.isUpsert();
        ArrayFilters arrayFilters = ArrayFilters.empty();

        clearLastStatus(channel);
        try {
            Document result = updateDocuments(collectionName, selector, update, arrayFilters, multi, upsert);
            putLastResult(channel, result);
        } catch (MongoServerException e) {
            putLastError(channel, e);
            log.error("failed to update {}", updateCommand, e);
        }
    }

    private void addIndex(Document indexDescription) {
        if (!indexDescription.containsKey("v")) {
            indexDescription.put("v", 2);
        }
        openOrCreateIndex(indexDescription);
        try {
        	getOrCreateIndexesCollection().addDocument(indexDescription);
        }
        catch(MongoServerError e) {
        	log.error("failed to addIndex {}", indexDescription, e);
        }
    }

    private MongoCollection<P> getOrCreateIndexesCollection() {
        synchronized (indexes) {
            if (indexes.get() == null) {
                MongoCollection<P> indexCollection = openOrCreateCollection(INDEXES_COLLECTION_NAME, ID_FIELD);
               
                addNamespace(indexCollection);
                indexes.set(indexCollection);
                
                Document indexDescription = new Document("ns",indexCollection.getDatabaseName()+'.'+indexCollection.getCollectionName());
                indexDescription.append("unique", 1);
                indexDescription.append("name", "ns_name_1");
                Document keys = new Document();
                keys.append("ns", 1).append("name", 1);
                indexDescription.append("key", keys);
                openOrCreateIndex(indexDescription);
            }
            return indexes.get();
        }
    }

    protected String extractCollectionNameFromNamespace(String namespace) {
    	if(namespace.startsWith(databaseName+'.')) {
    		Assert.startsWith(namespace, databaseName);
    		return namespace.substring(databaseName.length() + 1);
    	}
    	return namespace;
    }

    private void openOrCreateIndex(Document indexDescription) {
        String ns = indexDescription.get("ns").toString();
        String collectionName = extractCollectionNameFromNamespace(ns);

        MongoCollection<P> collection = resolveOrCreateCollection(collectionName);

        String indexName = (String) indexDescription.get("name");
        Document key = (Document) indexDescription.get("key");
        if (key.keySet().equals(Collections.singleton(ID_FIELD))) {
            boolean ascending = isAscending(key.get(ID_FIELD));
            Index<P> index = openOrCreateIdIndex(collectionName, indexName, ascending);
            log.info("adding unique _id index for collection {}", collectionName);
            collection.addIndex(index);
        } else if (Utils.isTrue(indexDescription.get("unique"))) {
            List<IndexKey> keys = new ArrayList<>();
            for (Entry<String, Object> entry : key.entrySet()) {
                String field = entry.getKey();
                boolean ascending = isAscending(entry.getValue());
                keys.add(new IndexKey(field, ascending));
            }

            boolean sparse = Utils.isTrue(indexDescription.get("sparse"));
            log.info("adding {} unique index {} for collection {}", sparse ? "sparse" : "non-sparse", keys, collectionName);

            Index<P> index = openOrCreateUniqueIndex(collectionName, indexName, keys, sparse);
            collection.addIndex(index);
        } else {        	
        	List<IndexKey> keys = new ArrayList<>();
            for (Entry<String, Object> entry : key.entrySet()) {
                String field = entry.getKey();
                boolean ascending = isAscending(entry.getValue());
                keys.add(new IndexKey(field, ascending));
            }

            boolean sparse = Utils.isTrue(indexDescription.get("sparse"));
        	Index<P> index = openOrCreateIndex(collectionName, indexName, keys, sparse);
        	if(index==null) {
	            // TODO: non-unique non-id indexes not yet implemented
	            log.warn("adding non-unique non-id index with key {} is not yet implemented", key);
        	}
        	else {
        		log.info("adding {} index {} for collection {}", sparse ? "sparse" : "non-sparse", keys, collectionName);

        		collection.addIndex(index);
        	}
        }
    }

    private static boolean isAscending(Object keyValue) {
        return Objects.equals(Utils.normalizeValue(keyValue), Double.valueOf(1.0));
    }

    private Index<P> openOrCreateIdIndex(String collectionName, String indexName, boolean ascending) {
        return openOrCreateUniqueIndex(collectionName, indexName, Collections.singletonList(new IndexKey(ID_FIELD, ascending)), false);
    }

    protected Index<P> openOrCreateIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
       return null;
    }
    
    protected abstract Index<P> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse);

    private void insertDocuments(Channel channel, String collectionName, List<Document> documents) {
        clearLastStatus(channel);
        try {
            if (isSystemCollection(collectionName)) {
                throw new MongoServerError(16459, "attempt to insert in system namespace");
            }
            MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
            collection.insertDocuments(documents);
            Document result = new Document("n", 0);
            result.put("err", null);
            putLastResult(channel, result);
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private Document deleteDocuments(Channel channel, String collectionName, Document selector, int limit) {
        clearLastStatus(channel);
        try {
            if (isSystemCollection(collectionName)) {
                throw new MongoServerError(73, "InvalidNamespace",
                    "cannot write to '" + getDatabaseName() + "." + collectionName + "'");
            }
            MongoCollection<P> collection = resolveCollection(collectionName, false);
            final int n;
            if (collection == null) {
                n = 0;
            } else {
                n = collection.deleteDocuments(selector, limit);
            }
            Document result = new Document("n", Integer.valueOf(n));
            putLastResult(channel, result);
            return result;
        } catch (MongoServerError e) {
            putLastError(channel, e);
            throw e;
        }
    }

    private Document updateDocuments(String collectionName, Document selector,
                                     Document update, ArrayFilters arrayFilters,
                                     boolean multi, boolean upsert) {

        if (isSystemCollection(collectionName)) {
            throw new MongoServerError(10156, "cannot update system collection");
        }

        MongoCollection<P> collection = resolveOrCreateCollection(collectionName);
        return collection.updateDocuments(selector, update, arrayFilters, multi, upsert);
    }

    private void putLastError(Channel channel, MongoServerException ex) {
        Document error = toError(channel, ex);
        putLastResult(channel, error);
    }

    private Document toWriteError(int index, MongoServerException e) {
        Document error = new Document();
        error.put("index", index);
        error.put("errmsg", e.getMessageWithoutErrorCode());
        if (e instanceof MongoServerError) {
            MongoServerError err = (MongoServerError) e;
            error.put("code", Integer.valueOf(err.getCode()));
            error.putIfNotNull("codeName", err.getCodeName());
        }
        return error;
    }

    private Document toError(Channel channel, MongoServerException ex) {
        Document error = new Document();
        error.put("err", ex.getMessageWithoutErrorCode());
        if (ex instanceof MongoServerError) {
            MongoServerError err = (MongoServerError) ex;
            error.put("code", Integer.valueOf(err.getCode()));
            error.putIfNotNull("codeName", err.getCodeName());
        }
        error.put("connectionId", channel.id().asShortText());
        return error;
    }

    private synchronized void putLastResult(Channel channel, Document result) {
        List<Document> results = lastResults.get(channel);
        // list must not be empty
        Document last = results.get(results.size() - 1);
        Assert.isNull(last, () -> "last result already set: " + last);
        results.set(results.size() - 1, result);
    }

    protected MongoCollection<P> createCollection(String collectionName) {
        checkCollectionName(collectionName);
        if (collectionName.contains("$")) {
            throw new MongoServerError(10093, "cannot insert into reserved $ collection");
        }

        MongoCollection<P> collection = openOrCreateCollection(collectionName, ID_FIELD);
        addNamespace(collection);

        addIndex(getPrimaryKeyIndexDescription(collection.getFullName()));

        log.info("created collection {}", collection.getFullName());

        return collection;
    }

    protected abstract MongoCollection<P> openOrCreateCollection(String collectionName, String idField);

    @Override
    public void drop() {
        log.debug("dropping {}", this);
        for (String collectionName : collections.keySet()) {
            if (!isSystemCollection(collectionName)) {
                dropCollection(collectionName);
            }
        }
        dropCollectionIfExists(INDEXES_COLLECTION_NAME);
        dropCollectionIfExists(NAMESPACES_COLLECTION_NAME);
    }

    private void dropCollectionIfExists(String collectionName) {
        if (collections.containsKey(collectionName)) {
            dropCollection(collectionName);
        }
    }

    @Override
    public void dropCollection(String collectionName) {
        MongoCollection<P> collection = resolveCollection(collectionName, true);
        dropAllIndexes(collection);
        collection.drop();
        unregisterCollection(collectionName);
    }

    private void dropAllIndexes(MongoCollection<P> collection) {
        MongoCollection<P> indexCollection = indexes.get();
        if (indexCollection == null) {
            return;
        }
        List<Document> indexesToDrop = new ArrayList<>();
        for (Document index : indexCollection.handleQuery(new Document("ns", collection.getFullName()))) {
            indexesToDrop.add(index);
        }
        for (Document indexToDrop : indexesToDrop) {
            dropIndex(collection, indexToDrop);
        }
    }

    @Override
    public void unregisterCollection(String collectionName) {
        MongoCollection<P> removedCollection = collections.remove(collectionName);
        namespaces.deleteDocuments(new Document("name", removedCollection.getFullName()), 1);
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        String oldFullName = collection.getFullName();
        oldDatabase.unregisterCollection(collection.getCollectionName());
        collection.renameTo(this, newCollectionName);
        // TODO resolve cast
        @SuppressWarnings("unchecked")
        MongoCollection<P> newCollection = (MongoCollection<P>) collection;
        collections.put(newCollectionName, newCollection);
        List<Document> newDocuments = new ArrayList<>();
        newDocuments.add(new Document("name", collection.getFullName()));

        indexes.get().updateDocuments(new Document("ns", oldFullName),
            new Document("$set", new Document("ns", newCollection.getFullName())),
            ArrayFilters.empty(), true, false);

        namespaces.insertDocuments(newDocuments);
    }

    public static boolean isSystemCollection(String collectionName) {
        return collectionName.startsWith("system.");
    }

}
