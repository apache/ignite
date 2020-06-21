package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CannotIndexParallelArraysError;
import de.bwaldvogel.mongo.exception.KeyConstraintError;

public abstract class Index<P> {

    private final String name;
    private final List<IndexKey> keys;
    private final boolean sparse;
    
    private LinkedHashSet<String> keysSet = null;
    private List<String> keysList = null;

    protected Index(String name, List<IndexKey> keys, boolean sparse) {
        this.name = name;
        this.keys = keys;
        this.sparse = sparse;
    }

    protected boolean isSparse() {
        return sparse;
    }

    public List<IndexKey> getKeys() {
        return keys;
    }

    public boolean hasSameOptions(Index<?> other) {
        return sparse == other.sparse;
    }

    public String getName() {
        return name;
    }

    public List<String> keys() {
    	if(keysList==null) {
    		keysList = keys.stream()
    	            .map(IndexKey::getKey)
    	            .collect(Collectors.toList());
    	}
        return keysList;
    }

    public Set<String> keySet() {
    	if(keysSet == null) {
    		keysSet = keys.stream()
    	            .map(IndexKey::getKey)
    	            .collect(Collectors.toCollection(LinkedHashSet::new));    		
    	}
    	return keysSet;
    }

    public Set<KeyValue> getKeyValues(Document document) {
        return getKeyValues(document, true);
    }

    Set<KeyValue> getKeyValues(Document document, boolean normalize) {
        Map<String, Object> valuesPerKey = collectValuesPerKey(document);
        if (normalize) {
            valuesPerKey.replaceAll((key, value) -> Utils.normalizeValue(value));
        }

        List<Collection<?>> collectionValues = valuesPerKey.values().stream()
            .filter(value -> value instanceof Collection)
            .map(value -> (Collection<?>) value)
            .collect(Collectors.toList());

        if (collectionValues.size() == 1) {
            @SuppressWarnings("unchecked")
            Collection<Object> collectionValue = (Collection<Object>) CollectionUtils.getSingleElement(collectionValues);
            return CollectionUtils.multiplyWithOtherElements(valuesPerKey.values(), collectionValue).stream()
                .map(KeyValue::new)
                .collect(StreamUtils.toLinkedHashSet());
        } else if (collectionValues.size() > 1) {
            validateHasNoParallelArrays(document);
            return collectCollectionValues(collectionValues);
        } else {
            return Collections.singleton(KeyValue.valueOf(valuesPerKey.values().toArray()));
        }
    }

    private void validateHasNoParallelArrays(Document document) {
        Set<List<String>> arrayPaths = new LinkedHashSet<>();
        for (String key : keys()) {
            List<String> pathToFirstCollection = getPathToFirstCollection(document, key);
            if (pathToFirstCollection != null) {
                arrayPaths.add(pathToFirstCollection);
            }
        }
        if (arrayPaths.size() > 1) {
            List<String> parallelArraysPaths = arrayPaths.stream()
                .map(path -> path.get(path.size() - 1))
                .collect(Collectors.toList());
            throw new CannotIndexParallelArraysError(parallelArraysPaths);
        }
    }

    private static List<String> getPathToFirstCollection(Document document, String key) {
        List<String> fragments = Utils.splitPath(key);
        List<String> remainingFragments = Utils.getTail(fragments);
        return getPathToFirstCollection(document, remainingFragments, Collections.singletonList(fragments.get(0)));
    }

    private static List<String> getPathToFirstCollection(Document document, List<String> remainingFragments, List<String> path) {
        Object value = Utils.getSubdocumentValue(document, Utils.joinPath(path));
        if (value instanceof Collection) {
            return path;
        }
        if (remainingFragments.isEmpty()) {
            return null;
        }
        List<String> newPath = new ArrayList<>(path);
        newPath.add(remainingFragments.get(0));
        return getPathToFirstCollection(document, Utils.getTail(remainingFragments), newPath);
    }

    private Map<String, Object> collectValuesPerKey(Document document) {
        Map<String, Object> valuesPerKey = new LinkedHashMap<>();
        for (String key : keys()) {
            Object value = Utils.getSubdocumentValueCollectionAware(document, key);
            valuesPerKey.put(key, value);
        }
        return valuesPerKey;
    }

    private static Set<KeyValue> collectCollectionValues(List<Collection<?>> collectionValues) {
        int size = collectionValues.get(0).size();
        Set<KeyValue> keyValues = new LinkedHashSet<>();
        for (int i = 0; i < size; i++) {
            int pos = i;
            List<Object> values = collectionValues.stream()
                .map(collection -> CollectionUtils.getElementAtPosition(collection, pos))
                .collect(Collectors.toList());
            keyValues.add(KeyValue.valueOf(values.toArray()));
        }
        return keyValues;
    }

    public abstract P getPosition(Document document);

    public abstract void checkAdd(Document document, MongoCollection<P> collection);

    public abstract void add(Document document, P position, MongoCollection<P> collection);

    public abstract P remove(Document document,MongoCollection<P> collection);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<P> getPositions(Document query);

    public abstract long getCount();

    public boolean isEmpty() {
        return getCount() == 0;
    }

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection);

    public abstract void updateInPlace(Document oldDocument, Document newDocument, P position, MongoCollection<P> collection) throws KeyConstraintError;

    protected boolean isCompoundIndex() {
        return keys().size() > 1;
    }

    protected boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Set<KeyValue> oldKeyValues = getKeyValues(oldDocument);
        Set<KeyValue> newKeyValues = getKeyValues(newDocument);
        return Utils.nullAwareEquals(oldKeyValues, newKeyValues);
    }

    public abstract void drop();

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[name=" + getName() + "]";
    }


    protected KeyValue getQueriedKeys(Document query) {
    	List<String> keys = keys();
    	Object[] values = new Object[keys.size()];
    	for(int i=0;i<values.length;i++) {
    		values[i] = Utils.normalizeValue(query.get(keys.get(i)));
    	}
        return KeyValue.valueOf(values);
    }
}
