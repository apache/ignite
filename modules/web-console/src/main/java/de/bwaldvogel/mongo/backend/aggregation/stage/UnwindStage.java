package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class UnwindStage implements AggregationStage {

    private final String path;
    private boolean preserveNullAndEmptyArrays;
    private String includeArrayIndex;

    public UnwindStage(Object input) {
        if (!(input instanceof String) && !(input instanceof Document)) {
            throw new MongoServerError(15981,
                "expected either a string or an object as specification for $unwind stage, got " + describeType(input));
        }

        final String fieldPath;
        if (input instanceof Document) {
            Document inputDocument = (Document) input;
            if (!inputDocument.containsKey("path")) {
                throw new MongoServerError(28812, "no path specified to $unwind stage");
            }

            Object path = inputDocument.get("path");
            if (!(path instanceof String)) {
                throw new MongoServerError(28808, "expected a string as the path for $unwind stage, got " + describeType(path));
            }
            fieldPath = (String) path;

            preserveNullAndEmptyArrays = Utils.isTrue(inputDocument.get("preserveNullAndEmptyArrays"));
            includeArrayIndex = (String) inputDocument.get("includeArrayIndex");
        } else {
            fieldPath = (String) input;
        }
        if (!fieldPath.startsWith("$")) {
            throw new MongoServerError(28818, "path option to $unwind stage should be prefixed with a '$': " + fieldPath);
        }
        this.path = fieldPath.substring(1);
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.flatMap(document -> {
            Object values = Utils.getSubdocumentValue(document, path);
            if (Missing.isNullOrMissing(values)) {
                if (preserveNullAndEmptyArrays) {
                    return streamWithoutIndex(document);
                } else {
                    return streamWithoutIndex();
                }
            } else if (values instanceof Collection) {
                Collection<?> collection = (Collection<?>) values;
                if (collection.isEmpty() && preserveNullAndEmptyArrays) {
                    Document documentClone = document.cloneDeeply();
                    Utils.removeSubdocumentValue(documentClone, path);
                    return streamWithoutIndex(documentClone);
                }
                return streamWithIndex(collection.stream()
                    .map(collectionValue -> {
                        Document documentClone = document.cloneDeeply();
                        Utils.changeSubdocumentValue(documentClone, path, collectionValue);
                        return documentClone;
                    }));
            } else {
                return streamWithoutIndex(document);
            }
        });
    }

    private Stream<Document> streamWithoutIndex(Document... documents) {
        if (includeArrayIndex == null) {
            return Stream.of(documents);
        } else {
            return Stream.of(documents).map(document -> {
                Document documentClone = document.cloneDeeply();
                Utils.changeSubdocumentValue(documentClone, includeArrayIndex, null);
                return documentClone;
            });
        }
    }

    private Stream<Document> streamWithIndex(Stream<Document> documents) {
        if (includeArrayIndex == null) {
            return documents;
        } else {
            AtomicLong counter = new AtomicLong();
            return documents.peek(document -> {
                long index = counter.getAndIncrement();
                Utils.changeSubdocumentValue(document, includeArrayIndex, index);
            });
        }
    }

}
