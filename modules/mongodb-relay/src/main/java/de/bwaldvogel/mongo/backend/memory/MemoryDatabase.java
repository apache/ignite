package de.bwaldvogel.mongo.backend.memory;

import java.util.List;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.AbstractMongoDatabase;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.memory.index.MemoryUniqueIndex;

public class MemoryDatabase extends AbstractMongoDatabase<Integer> {

    public MemoryDatabase(MongoBackend backend, String databaseName) {
        super(databaseName, backend);
        initializeNamespacesAndIndexes();
    }

    @Override
    protected MemoryCollection openOrCreateCollection(String collectionName, String idField) {
        return new MemoryCollection(this, collectionName, idField);
    }

    @Override
    protected Index<Integer> openOrCreateUniqueIndex(String collectionName, String indexName, List<IndexKey> keys, boolean sparse) {
        return new MemoryUniqueIndex(indexName, keys, sparse);
    }

    @Override
    protected long getStorageSize() {
        return 0;
    }

    @Override
    protected long getFileSize() {
        return 0;
    }

}
