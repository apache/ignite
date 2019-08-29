package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class AbstractUniqueIndex<P> extends Index<P> {

    protected AbstractUniqueIndex(List<IndexKey> keys, boolean sparse) {
        super(keys, sparse);
    }

    protected abstract P removeDocument(KeyValue keyValue);

    protected boolean containsKey(KeyValue keyValue) {
        return getPosition(keyValue) != null;
    }

    protected abstract boolean putKeyPosition(KeyValue keyValue, P position);

    protected abstract Iterable<Entry<KeyValue, P>> getIterable();

    protected abstract P getPosition(KeyValue keyValue);

    private boolean isSparseAndHasNoValueForKeys(Document document) {
        return isSparse() && hasNoValueForKeys(document);
    }

    private boolean hasNoValueForKeys(Document document) {
        return keys().stream().noneMatch(key -> Utils.hasSubdocumentValue(document, key));
    }

    @Override
    public synchronized P remove(Document document) {
        if (isSparseAndHasNoValueForKeys(document)) {
            return null;
        }

        Set<KeyValue> keyValues = getKeyValues(document);
        Set<P> positions = keyValues.stream()
            .map(this::removeDocument)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());

        if (positions.isEmpty()) {
            return null;
        } else {
            return CollectionUtils.getSingleElement(positions);
        }
    }

    @Override
    public synchronized void checkAdd(Document document, MongoCollection<P> collection) {
        if (isSparseAndHasNoValueForKeys(document)) {
            return;
        }
        for (KeyValue key : getKeyValues(document, false)) {
            KeyValue normalizedKey = key.normalized();
            if (containsKey(normalizedKey)) {
                throw new DuplicateKeyError(this, collection, key);
            }
        }
    }

    @Override
    public synchronized void add(Document document, P position, MongoCollection<P> collection) {
        checkAdd(document, collection);
        if (isSparseAndHasNoValueForKeys(document)) {
            return;
        }
        Set<KeyValue> keyValues = getKeyValues(document);
        for (KeyValue keyValue : keyValues) {
            boolean added = putKeyPosition(keyValue, position);
            Assert.isTrue(added, () -> "Key " + keyValue + " already exists. Concurrency issue?");
        }
    }

    @Override
    public synchronized void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection) {
        if (nullAwareEqualsKeys(oldDocument, newDocument)) {
            return;
        }
        if (isSparseAndHasNoValueForKeys(newDocument)) {
            return;
        }
        P oldPosition = getDocumentPosition(oldDocument);
        for (KeyValue key : getKeyValues(newDocument, false)) {
            KeyValue normalizedKey = key.normalized();
            P position = getPosition(normalizedKey);
            if (position != null && !position.equals(oldPosition)) {
                throw new DuplicateKeyError(this, collection, key);
            }
        }
    }

    private P getDocumentPosition(Document oldDocument) {
        Set<P> positions = getKeyValues(oldDocument).stream()
            .map(this::getPosition)
            .collect(StreamUtils.toLinkedHashSet());
        return CollectionUtils.getSingleElement(positions);
    }

    @Override
    public void updateInPlace(Document oldDocument, Document newDocument, MongoCollection<P> collection) throws KeyConstraintError {
        if (!nullAwareEqualsKeys(oldDocument, newDocument)) {
            P position = remove(oldDocument);
            add(newDocument, position, collection);
        }
    }

    @Override
    public synchronized boolean canHandle(Document query) {

        if (!query.keySet().equals(keySet())) {
            return false;
        }

        if (isSparse() && query.values().stream().allMatch(Objects::isNull)) {
            return false;
        }

        for (String key : keys()) {
            Object queryValue = query.get(key);
            if (queryValue instanceof Document) {
                if (isCompoundIndex()) {
                    // https://github.com/bwaldvogel/mongo-java-server/issues/80
                    // Not yet supported. Use some other index, or none:
                    return false;
                }
                if (BsonRegularExpression.isRegularExpression(queryValue)) {
                    return true;
                }
                for (String queriedKeys : ((Document) queryValue).keySet()) {
                    if (isInQuery(queriedKeys)) {
                        // okay
                    } else if (queriedKeys.startsWith("$")) {
                        // not yet supported
                        return false;
                    }
                }
            }
        }

        return true;
    }

    private static boolean isInQuery(String key) {
        return key.equals(QueryOperator.IN.getValue());
    }

    @Override
    public synchronized Iterable<P> getPositions(Document query) {
        KeyValue queriedKeys = getQueriedKeys(query);

        for (Object queriedKey : queriedKeys) {
            if (BsonRegularExpression.isRegularExpression(queriedKey)) {
                if (isCompoundIndex()) {
                    throw new UnsupportedOperationException("Not yet implemented");
                }
                List<P> positions = new ArrayList<>();
                for (Entry<KeyValue, P> entry : getIterable()) {
                    KeyValue obj = entry.getKey();
                    if (obj.size() == 1) {
                        Object o = obj.get(0);
                        if (o instanceof String) {
                            BsonRegularExpression regularExpression = BsonRegularExpression.convertToRegularExpression(queriedKey);
                            Matcher matcher = regularExpression.matcher(o.toString());
                            if (matcher.find()) {
                                positions.add(entry.getValue());
                            }
                        }
                    }
                }
                return positions;
            } else if (queriedKey instanceof Document) {
                if (isCompoundIndex()) {
                    throw new UnsupportedOperationException("Not yet implemented");
                }
                Document keyObj = (Document) queriedKey;
                if (Utils.containsQueryExpression(keyObj)) {
                    String expression = CollectionUtils.getSingleElement(keyObj.keySet(),
                        () -> new UnsupportedOperationException("illegal query key: " + queriedKeys));

                    if (expression.startsWith("$")) {
                        return getPositionsForExpression(keyObj, expression);
                    }
                }
            }
        }

        P position = getPosition(queriedKeys);
        if (position == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(position);
    }

    private KeyValue getQueriedKeys(Document query) {
        return new KeyValue(keys().stream()
            .map(query::get)
            .map(Utils::normalizeValue)
            .collect(Collectors.toList()));
    }

    private Iterable<P> getPositionsForExpression(Document keyObj, String operator) {
        if (isInQuery(operator)) {
            @SuppressWarnings("unchecked")
            Collection<Object> objects = (Collection<Object>) keyObj.get(operator);
            Collection<Object> queriedObjects = new TreeSet<>(ValueComparator.asc());
            queriedObjects.addAll(objects);

            List<P> allKeys = new ArrayList<>();
            for (Object object : queriedObjects) {
                Object keyValue = Utils.normalizeValue(object);
                P key = getPosition(new KeyValue(keyValue));
                if (key != null) {
                    allKeys.add(key);
                }
            }

            return allKeys;
        } else {
            throw new UnsupportedOperationException("unsupported query expression: " + operator);
        }
    }

}
