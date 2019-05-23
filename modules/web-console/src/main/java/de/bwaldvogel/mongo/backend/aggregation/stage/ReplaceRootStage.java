package de.bwaldvogel.mongo.backend.aggregation.stage;

import static de.bwaldvogel.mongo.backend.Utils.describeType;

import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.Json;
import de.bwaldvogel.mongo.exception.MongoServerError;

public class ReplaceRootStage implements AggregationStage {

    private final Object newRoot;

    public ReplaceRootStage(Document document) {
        newRoot = document.getOrMissing("newRoot");

        if (Missing.isNullOrMissing(newRoot)) {
            throw new MongoServerError(40231, "no newRoot specified for the $replaceRoot stage");
        }
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::replaceRoot);
    }

    Document replaceRoot(Document document) {
        Object evaluatedNewRoot = Expression.evaluateDocument(newRoot, document);

        if (!(evaluatedNewRoot instanceof Document)) {
            throw new MongoServerError(40228, "'newRoot' expression must evaluate to an object, but resulting value was: " + Json.toJsonValue(evaluatedNewRoot, true, "{", "}")
                + ". Type of resulting value: '" + describeType(evaluatedNewRoot)
                + "'. Input document: " + document.toString(true));
        }

        return (Document) evaluatedNewRoot;
    }
}
