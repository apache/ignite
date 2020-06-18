package de.bwaldvogel.mongo.backend;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class DefaultQueryMatcher implements QueryMatcher {

    private static final Logger log = LoggerFactory.getLogger(DefaultQueryMatcher.class);

    private Integer lastPosition;

    @Override
    public boolean matches(Document document, Document query) {
        for (String key : query.keySet()) {
            Object queryValue = query.get(key);
            validateQueryValue(queryValue, key);
            if (!checkMatch(queryValue, key, document)) {
                return false;
            }
        }

        return true;
    }

    private void validateQueryValue(Object queryValue, String key) {
        if (!(queryValue instanceof Document)) {
            return;
        }
        if (BsonRegularExpression.isRegularExpression(queryValue)) {
            return;
        }
        Document queryObject = (Document) queryValue;
        if (!queryObject.keySet().isEmpty() && queryObject.keySet().iterator().next().startsWith("$")) {
            for (String operator : queryObject.keySet()) {
                if (Constants.REFERENCE_KEYS.contains(operator)) {
                    continue;
                }
                QueryOperator queryOperator = QueryOperator.fromValue(operator);
                if (queryOperator == QueryOperator.TYPE) {
                    Object value = queryObject.get(operator);
                    if (value instanceof Collection) {
                        Collection<?> values = (Collection<?>) value;
                        if (values.isEmpty()) {
                            throw new FailedToParseException(key + " must match at least one type");
                        }
                    }
                }
            }
        }
    }

    @Override
    public synchronized Integer matchPosition(Document document, Document query) {
        lastPosition = null;
        for (String key : query.keySet()) {
            if (!checkMatch(query.get(key), key, document)) {
                return null;
            }
        }

        return lastPosition;
    }

    private List<String> splitKey(String key) {
        List<String> keys = Arrays.asList(key.split("\\.", -1));
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).isEmpty() && i != keys.size() - 1) {
                log.warn("Illegal key: '{}'", key);
                return Collections.singletonList(key);
            }
        }
        return keys;
    }

    private boolean checkMatch(Object queryValue, String key, Object document) {
        return checkMatch(queryValue, splitKey(key), document);
    }

    private boolean checkMatch(Object queryValue, List<String> keys, Object value) {
        if (keys.isEmpty()) {
            throw new MongoServerException("illegal keys: " + keys);
        }

        String firstKey = keys.get(0);

        if (firstKey.equals("$comment")) {
            log.debug("query comment: '{}'", queryValue);
            return true;
        }

        List<String> subKeys = Collections.emptyList();
        if (keys.size() > 1) {
            subKeys = keys.subList(1, keys.size());
        }

        if (QueryFilter.isQueryFilter(firstKey)) {
            QueryFilter filter = QueryFilter.fromValue(firstKey);
            return checkMatch(queryValue, filter, value);
        } else if (firstKey.startsWith("$") && !Constants.REFERENCE_KEYS.contains(firstKey)) {
            throw new BadValueException("unknown top level operator: " + firstKey);
        }

        if (value instanceof List<?>) {
            if (firstKey.matches("\\d+")) {
                Object listValue = Utils.getFieldValueListSafe(value, firstKey);
                if (subKeys.isEmpty()) {
                    return checkMatchesValue(queryValue, listValue);
                } else {
                    return checkMatch(queryValue, subKeys, listValue);
                }
            } else if (firstKey.isEmpty()) {
                Assert.isEmpty(subKeys);
                return checkMatchesValue(queryValue, value);
            }

            // handle $all
            if (queryValue instanceof Document && ((Document) queryValue).keySet().contains(QueryOperator.ALL.getValue())) {
                // clone first
                queryValue = ((Document) queryValue).clone();
                Object allQuery = ((Document) queryValue).remove(QueryOperator.ALL.getValue());
                if (!checkMatchesAllDocuments(allQuery, keys, value)) {
                    return false;
                }
                // continue matching the remainder of queryValue
            }

            return checkMatchesAnyDocument(queryValue, keys, value);
        }

        if (!subKeys.isEmpty()) {
            Object subObject = Utils.getFieldValueListSafe(value, firstKey);
            return checkMatch(queryValue, subKeys, subObject);
        }

        final Document document;
        final Object documentValue;
        if (Missing.isNullOrMissing(value)) {
            document = null;
            documentValue = Missing.getInstance();
        } else if (value instanceof Document) {
            document = (Document) value;
            documentValue = document.getOrMissing(firstKey);
        } else {
            return checkMatchesValue(queryValue, Missing.getInstance());
        }

        if (documentValue instanceof Collection<?>) {
            Collection<?> documentValues = (Collection<?>) documentValue;
            if (queryValue instanceof Document) {
                Document queryDocument = (Document) queryValue;
                boolean matches = checkMatchesAnyValue(queryDocument, keys, document, documentValues);
                if (matches) {
                    return true;
                }
                if (isInQuery(queryDocument)) {
                    return checkMatchesValue(queryValue, documentValue);
                } else {
                    return false;
                }
            } else if (queryValue instanceof Collection<?>) {
                return checkMatchesValue(queryValue, documentValues);
            } else if (checkMatchesAnyValue(queryValue, documentValues)) {
                return true;
            }
        }

        return checkMatchesValue(queryValue, documentValue);
    }

    private static boolean isInQuery(Document queryDocument) {
        return queryDocument.keySet().equals(Collections.singleton(QueryOperator.IN.getValue()));
    }

    private boolean checkMatchesAnyValue(Document queryValue, List<String> keys, Document document, Collection<?> value) {
        Set<String> keySet = queryValue.keySet();

        // clone first
        Document queryValueClone = queryValue.clone();

        for (String queryOperator : keySet) {

            Object subQuery = queryValueClone.remove(queryOperator);

            if (queryOperator.equals(QueryOperator.ALL.getValue())) {
                if (!checkMatchesAllValues(subQuery, value)) {
                    return false;
                }
            } else if (queryOperator.equals(QueryOperator.IN.getValue())) {
                Document inQuery = new Document(queryOperator, subQuery);
                if (!checkMatchesAnyValue(inQuery, value)) {
                    return false;
                }
            } else if (queryOperator.equals(QueryOperator.NOT_IN.getValue())) {
                Document inQuery = new Document(QueryOperator.IN.getValue(), subQuery);
                if (checkMatchesAnyValue(inQuery, value)) {
                    return false;
                }
            } else if (queryOperator.equals(QueryOperator.NOT.getValue())) {
                if (checkMatch(subQuery, keys, document)) {
                    return false;
                }
            } else if (queryOperator.equals(QueryOperator.NOT_EQUALS.getValue())) {
                Document equalQuery = new Document(QueryOperator.EQUAL.getValue(), subQuery);
                if (subQuery instanceof Collection) {
                    if (checkMatchesValue(subQuery, value)) {
                        return false;
                    }
                } else if (checkMatchesAnyValue(equalQuery, value)) {
                    return false;
                }
            } else if (queryOperator.equals(QueryOperator.SIZE.getValue())) {
                Document sizeQuery = new Document(QueryOperator.SIZE.getValue(), subQuery);
                if (!checkMatchesValue(sizeQuery, value)) {
                    return false;
                }
            } else {
                if (!checkMatchesAnyValue(queryValue, value) && !checkMatchesValue(queryValue, value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean checkMatch(Object queryValue, QueryFilter filter, Object document) {
        if (filter == QueryFilter.EXPR) {
            Object result = Expression.evaluateDocument(queryValue, (Document) document);
            return Utils.isTrue(result);
        }

        if (!(queryValue instanceof List<?>)) {
            throw new BadValueException("$and/$or/$nor must be a nonempty array");
        }

        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) queryValue;
        if (list.isEmpty()) {
            throw new BadValueException("$and/$or/$nor must be a nonempty array");
        }

        for (Object subqueryValue : list) {
            if (!(subqueryValue instanceof Document)) {
                throw new MongoServerError(14817, filter + " elements must be objects");
            }
        }

        switch (filter) {
            case AND:
                for (Object subqueryValue : list) {
                    if (!matches((Document) document, (Document) subqueryValue)) {
                        return false;
                    }
                }
                return true;
            case OR:
                for (Object subqueryValue : list) {
                    if (matches((Document) document, (Document) subqueryValue)) {
                        return true;
                    }
                }
                return false;
            case NOR:
                return !checkMatch(queryValue, QueryFilter.OR, document);
            default:
                throw new MongoServerException("illegal query filter: " + filter + ". must not happen");
        }
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAllDocuments(Object queryValue, List<String> keys, Object document) {
        for (Object query : (Collection<Object>) queryValue) {
            if (!checkMatchesAnyDocument(query, keys, document)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesAnyDocument(Object queryValue, List<String> keys, Object document) {
        int i = 0;
        for (Object object : (Collection<Object>) document) {
            if (checkMatch(queryValue, keys, object)) {
                if (lastPosition == null) {
                    lastPosition = Integer.valueOf(i);
                }
                return true;
            }
            i++;
        }
        return false;
    }

    @Override
    public boolean matchesValue(Object queryValue, Object value) {
        return checkMatchesValue(queryValue, value, false);
    }

    private boolean checkMatchesValue(Object queryValue, Object value) {
        return checkMatchesValue(queryValue, value, true);
    }

    private boolean checkMatchesValue(Object queryValue, Object value, boolean requireExactMatch) {
        if (BsonRegularExpression.isRegularExpression(queryValue)) {
            if (Missing.isNullOrMissing(value)) {
                return false;
            } else {
                BsonRegularExpression pattern = BsonRegularExpression.convertToRegularExpression(queryValue);
                Matcher matcher = pattern.matcher(value.toString());
                return matcher.find();
            }
        }

        if (queryValue instanceof Document) {
            Document queryObject = (Document) queryValue;

            if (queryObject.keySet().equals(Constants.REFERENCE_KEYS)) {
                if (value instanceof Document) {
                    return matches((Document) value, queryObject);
                } else {
                    return false;
                }
            }

            if (requireExactMatch && value instanceof Document) {
                if (queryObject.keySet().stream().noneMatch(key -> key.startsWith("$"))) {
                    return Utils.nullAwareEquals(value, queryValue);
                }
            }

            for (String key : queryObject.keySet()) {
                Object querySubvalue = queryObject.get(key);
                if (key.startsWith("$")) {
                    if (!checkExpressionMatch(value, querySubvalue, key)) {
                        return false;
                    }
                } else if (Missing.isNullOrMissing(value) && querySubvalue == null) {
                    return false;
                } else if (!checkMatch(querySubvalue, key, value)) {
                    // the value of the query itself can be a complex query
                    return false;
                }
            }
            return true;
        }

        return Utils.nullAwareEquals(value, queryValue);
    }

    private boolean checkMatchesAllValues(Object queryValue, Object values) {
        if (!(queryValue instanceof Collection)) {
            return false;
        }

        Collection<?> list = (Collection<?>) values;
        Collection<?> queryValues = (Collection<?>) queryValue;

        if (queryValues.isEmpty()) {
            return false;
        }

        for (Object query : queryValues) {
            if (!checkMatchesAnyValue(query, list)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private boolean checkMatchesElemValues(Object queryValue, Object values) {
        if (!(queryValue instanceof Document)) {
            throw new BadValueException(QueryOperator.ELEM_MATCH.getValue() + " needs an Object");
        }
        if (!(values instanceof Collection)) {
            return false;
        }
        Collection<Object> list = (Collection<Object>) values;
        for (Object value : list) {
            if (checkMatchesValue(queryValue, value, false)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkMatchesAnyValue(Object queryValue, Collection<?> values) {
        if (queryValue instanceof Document) {
            Document queryDocument = (Document) queryValue;
            if (queryDocument.keySet().equals(Collections.singleton(QueryOperator.ELEM_MATCH.getValue()))) {
                queryValue = queryDocument.get(QueryOperator.ELEM_MATCH.getValue());
            }
        }

        int i = 0;
        for (Object value : values) {
            if (checkMatchesValue(queryValue, value)) {
                if (lastPosition == null) {
                    lastPosition = Integer.valueOf(i);
                }
                return true;
            }
            i++;
        }
        return false;
    }

    private boolean checkExpressionMatch(Object value, Object expressionValue, String operator) {

        if (QueryFilter.isQueryFilter(operator)) {
            QueryFilter filter = QueryFilter.fromValue(operator);
            return checkMatch(expressionValue, filter, value);
        }

        QueryOperator queryOperator = QueryOperator.fromValue(operator);

        switch (queryOperator) {
            case IN:
                Collection<?> queriedObjects = (Collection<?>) expressionValue;
                for (Object o : queriedObjects) {
                    if (o instanceof BsonRegularExpression && value instanceof String) {
                        BsonRegularExpression pattern = (BsonRegularExpression) o;
                        if (pattern.matcher((String) value).find()) {
                            return true;
                        }
                    } else if (value instanceof Collection && !(o instanceof Collection)) {
                        Collection<?> values = (Collection<?>) value;
                        return values.stream().anyMatch(v -> Utils.nullAwareEquals(o, v));
                    } else if (Utils.nullAwareEquals(o, value)) {
                        return true;
                    }
                }
                return false;
            case NOT:
                return !checkMatchesValue(expressionValue, value);
            case EQUAL:
                return Utils.nullAwareEquals(value, expressionValue);
            case NOT_EQUALS:
                return !Utils.nullAwareEquals(value, expressionValue);
            case NOT_IN:
                return !checkExpressionMatch(value, expressionValue, "$in");
            case EXISTS:
                return ((value instanceof Missing) != Utils.isTrue(expressionValue));
            case GREATER_THAN:
                if (!comparableTypes(value, expressionValue)) {
                    return false;
                }
                return ValueComparator.desc().compare(value, expressionValue) < 0;
            case GREATER_THAN_OR_EQUAL:
                if (!comparableTypes(value, expressionValue)) {
                    return false;
                }
                return ValueComparator.desc().compare(value, expressionValue) <= 0;
            case LESS_THAN:
                if (!comparableTypes(value, expressionValue)) {
                    return false;
                }
                return ValueComparator.asc().compare(value, expressionValue) < 0;
            case LESS_THAN_OR_EQUAL:
                if (!comparableTypes(value, expressionValue)) {
                    return false;
                }
                return ValueComparator.asc().compare(value, expressionValue) <= 0;
            case MOD: {
                if (!(value instanceof Number)) {
                    return false;
                }

                @SuppressWarnings("unchecked")
                List<Number> modValue = (List<Number>) expressionValue;
                return (((Number) value).intValue() % modValue.get(0).intValue() == modValue.get(1).intValue());
            }
            case SIZE: {
                if (!(expressionValue instanceof Number)) {
                    throw new BadValueException("$size needs a number");
                }
                if (!(value instanceof Collection<?>)) {
                    return false;
                }
                int listSize = ((Collection<?>) value).size();
                double matchingSize = ((Number) expressionValue).doubleValue();
                return listSize == matchingSize;
            }
            case ALL:
                return false;
            case TYPE:
                return matchTypes(value, expressionValue);
            case ELEM_MATCH:
                return checkMatchesElemValues(expressionValue, value);
            default:
                throw new IllegalArgumentException("unhandled query operator: " + queryOperator);
        }
    }

    static boolean matchTypes(Object value, Object expressionValue) {
        if (Objects.equals(expressionValue, "number")) {
            List<String> types = Stream.of(BsonType.INT, BsonType.LONG, BsonType.DOUBLE, BsonType.DECIMAL128)
                .map(BsonType::getAlias)
                .collect(Collectors.toList());
            return matchTypes(value, types);
        } else if (expressionValue instanceof String) {
            return matchTypes(value, BsonType.forString((String) expressionValue));
        } else if (expressionValue instanceof Number) {
            return matchTypes(value, BsonType.forNumber((Number) expressionValue));
        } else if (expressionValue instanceof Collection) {
            Collection<?> values = (Collection<?>) expressionValue;
            for (Object type : values) {
                if (matchTypes(value, type)) {
                    return true;
                }
            }
            return false;
        } else {
            throw new MongoServerError(14, "type must be represented as a number or a string");
        }
    }

    private static boolean matchTypes(Object value, BsonType type) {
        return type.matches(value);
    }

    private boolean comparableTypes(Object value, Object expressionValue) {
        if (expressionValue instanceof MinKey || expressionValue instanceof MaxKey) {
            return true;
        }
        value = Utils.normalizeValue(value);
        expressionValue = Utils.normalizeValue(expressionValue);
        if (value == null || expressionValue == null) {
            return false;
        }

        return value.getClass().equals(expressionValue.getClass());
    }
}
