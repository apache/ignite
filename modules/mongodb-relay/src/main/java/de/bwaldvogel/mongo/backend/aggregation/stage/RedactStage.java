package de.bwaldvogel.mongo.backend.aggregation.stage;

import java.util.Objects;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.TypeMismatchException;

public class RedactStage implements AggregationStage {

    private final Document expression;

    public RedactStage(Document expression) {
        this.expression = expression;
    }

    @Override
    public Stream<Document> apply(Stream<Document> stream) {
        return stream.map(this::redact).filter(Objects::nonNull);
    }

    private Document redact(Document document) {
        Object value = Expression.evaluateDocument(expression, document);
        if (!(value instanceof String)) {
            throw new TypeMismatchException("Result of $redact expression should be: " +
                "$$DESCEND, $$PRUNE or $$KEEP, but found " + value);
        }
        switch ((String) value) {
            case "$$DESCEND":
                throw new MongoServerError(ErrorCode.CommandNotFound, "$$DESCEND is not implemented yet");
            case "$$PRUNE":
                return null;
            case "$$KEEP":
                return document;
            default:
                throw new TypeMismatchException("Result of $redact expression should be: " +
                    "$$DESCEND, $$PRUNE or $$KEEP, but found " + value);
        }
    }
}
