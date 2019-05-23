package de.bwaldvogel.mongo.backend.h2;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class H2Database extends AbstractMongoDatabase<Object> {

    private static final String META_PREFIX = "meta.";
    static final String DATABASES_PREFIX = "databases.";

    private MVStore mvStore;

    public H2Database(String databaseName, MongoBackend backend, MVStore mvStore) {
        super(databaseName, backend);
        this.mvStore = mvStore;
        initializeNamespacesAndIndexes();
    }

    @Override
    protected Index<Object> openOrCreateUniqueIndex(String collectionName, List<IndexKey> keys, boolean sparse) {    	
        MVMap<List<Object>, Object> mvMap = mvStore.openMap(databaseName + "." + collectionName + "._index_" + indexName(keys));
        return new H2UniqueIndex(mvMap, keys, sparse);
    }

    @Override
    public void drop() {
        super.drop();

        List<MVMap<?, ?>> maps = mvStore.getMapNames().stream()
            .filter(name -> name.startsWith(databaseName + ".")
                || name.startsWith(DATABASES_PREFIX + databaseName)
                || name.startsWith(META_PREFIX + databaseName)
            )
            .map(mvStore::openMap)
            .collect(Collectors.toList());

        for (MVMap<?, ?> map : maps) {
            mvStore.removeMap(map);
        }
    }

    static String indexName(List<IndexKey> keys) {
        Assert.notEmpty(keys, () -> "No keys");
        return keys.stream()
            .map(k -> k.getKey() + "." + (k.isAscending() ? "ASC" : "DESC"))
            .collect(Collectors.joining("_"));
    }

    @Override
    protected MongoCollection<Object> openOrCreateCollection(String collectionName, String idField) {
        String fullCollectionName = databaseName + "." + collectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        return new H2Collection(databaseName, collectionName, idField, dataMap, metaMap);
    }

    @Override
    protected long getStorageSize() {
        FileStore fileStore = mvStore.getFileStore();
        if (fileStore != null) {
            try {
                return fileStore.getFile().size();
            } catch (IOException e) {
                throw new RuntimeException("Failed to calculate filestore size", e);
            }
        } else {
            return 0;
        }
    }

    @Override
    protected long getFileSize() {
        return getStorageSize();
    }

    @Override
    public void dropCollection(String collectionName) {
        super.dropCollection(collectionName);
        String fullCollectionName = getDatabaseName() + "." + collectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);
        mvStore.removeMap(dataMap);
        mvStore.removeMap(metaMap);
    }

    @Override
    public void moveCollection(MongoDatabase oldDatabase, MongoCollection<?> collection, String newCollectionName) {
        super.moveCollection(oldDatabase, collection, newCollectionName);
        String fullCollectionName = collection.getFullName();
        String newFullName = collection.getDatabaseName() + "." + newCollectionName;
        MVMap<Object, Document> dataMap = mvStore.openMap(DATABASES_PREFIX + fullCollectionName);
        MVMap<String, Object> metaMap = mvStore.openMap(META_PREFIX + fullCollectionName);

        mvStore.renameMap(dataMap, DATABASES_PREFIX + newFullName);
        mvStore.renameMap(metaMap, META_PREFIX + newFullName);
    }

}
