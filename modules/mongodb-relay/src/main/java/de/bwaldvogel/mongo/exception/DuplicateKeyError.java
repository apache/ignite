package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.KeyValue;

public class DuplicateKeyError extends KeyConstraintError {

    private static final long serialVersionUID = 1L;

    public DuplicateKeyError(Index<?> index, MongoCollection<?> collection, KeyValue keyValue) {
        this(collection.getFullName(), index.getName() + " dup key: " + keyValue);
    }

    public DuplicateKeyError(String collectionFullName, String message) {
        super(11000, "DuplicateKey",
            "E11000 duplicate key error collection: " + collectionFullName + " index: " + message);
    }

}
