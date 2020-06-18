package de.bwaldvogel.mongo.exception;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Json;

public class DuplicateKeyError extends KeyConstraintError {

    private static final long serialVersionUID = 1L;

    public DuplicateKeyError(Index<?> index, MongoCollection<?> collection, List<IndexKey> keys, KeyValue keyValue) {
        this(collection.getFullName(), index.getName() + " dup key: " + describeKeyValueToString(keys, keyValue));
    }

    private static String describeKeyValueToString(List<IndexKey> keys, KeyValue keyValue) {
        return IntStream.range(0, keys.size())
            .mapToObj(index -> {
                IndexKey key = keys.get(index);
                Object value = keyValue.get(index);
                return key.getKey() + ": " + Json.toJsonValue(value, true, "{ ", " }");
            })
            .collect(Collectors.joining(", ", "{ ", " }"));
    }

    public DuplicateKeyError(String collectionFullName, String message) {
        super(11000, "DuplicateKey",
            "E11000 duplicate key error collection: " + collectionFullName + " index: " + message);
    }

}
