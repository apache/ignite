package de.bwaldvogel.mongo.backend;

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

    private final List<IndexKey> keys;
    private final boolean sparse;

    protected Index(List<IndexKey> keys, boolean sparse) {
        this.keys = keys;
        this.sparse = sparse;
    }

    protected boolean isSparse() {
        return sparse;
    }

    protected List<IndexKey> getKeys() {
        return keys;
    }

    public String getName() {
        if (keys.size() == 1 && CollectionUtils.getSingleElement(keys).getKey().equals(Constants.ID_FIELD)) {
            return Constants.ID_INDEX_NAME;
        }
        return keys.stream()
            .map(indexKey -> indexKey.getKey() + "_" + (indexKey.isAscending() ? "1" : "-1"))
            .collect(Collectors.joining("_"));
    }

    protected List<String> keys() {
    	if(keys.size()==1) {
    		return Collections.singletonList(keys.get(0).getKey());
    	}
        return keys.stream()
            .map(IndexKey::getKey)
            .collect(Collectors.toList());
    }

    protected Set<String> keySet() {
    	if(keys.size()==1) {
    		return Collections.singleton(keys.get(0).getKey());
    	}
        return keys.stream()
            .map(IndexKey::getKey)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    Set<KeyValue> getKeyValues(Document document) {
        return getKeyValues(document, true);
    }

    Set<KeyValue> getKeyValues(Document document, boolean normalize) {
        Map<String, Object> valuesPerKey = new LinkedHashMap<>();
        for (String key : keys()) {
            Object value = Utils.getSubdocumentValueCollectionAware(document, key);
            if (normalize) {
                value = Utils.normalizeValue(value);
            }
            valuesPerKey.put(key, value);
        }

        Map<String, Object> collectionValues = valuesPerKey.entrySet().stream()
            .filter(entry -> entry.getValue() instanceof Collection)
            .collect(StreamUtils.toLinkedHashMap());

        if (collectionValues.size() == 1) {
            @SuppressWarnings("unchecked")
            Collection<Object> collectionValue = (Collection<Object>) CollectionUtils.getSingleElement(collectionValues.values());
            return CollectionUtils.multiplyWithOtherElements(valuesPerKey.values(), collectionValue).stream()
                .map(KeyValue::new)
                .collect(StreamUtils.toLinkedHashSet());
        } else if (collectionValues.size() > 1) {
            throw new CannotIndexParallelArraysError(collectionValues.keySet());
        } else {
            return Collections.singleton(new KeyValue(valuesPerKey.values()));
        }
    }

    public abstract void checkAdd(Document document, MongoCollection<P> collection);

    public abstract void add(Document document, P position, MongoCollection<P> collection);

    public abstract P remove(Document document);

    public abstract boolean canHandle(Document query);

    public abstract Iterable<P> getPositions(Document query);

    public abstract long getCount();

    public abstract boolean isEmpty();

    public abstract long getDataSize();

    public abstract void checkUpdate(Document oldDocument, Document newDocument, MongoCollection<P> collection);

    public abstract void updateInPlace(Document oldDocument, Document newDocument, MongoCollection<P> collection) throws KeyConstraintError;

    protected boolean isCompoundIndex() {
        return keys().size() > 1;
    }

    protected boolean nullAwareEqualsKeys(Document oldDocument, Document newDocument) {
        Set<KeyValue> oldKeyValues = getKeyValues(oldDocument);
        Set<KeyValue> newKeyValues = getKeyValues(newDocument);
        return Utils.nullAwareEquals(oldKeyValues, newKeyValues);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((keys == null) ? 0 : keys.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Index other = (Index) obj;
		if (keys == null) {
			if (other.keys != null)
				return false;
		} else if (!keys.equals(other.keys))
			return false;
		return true;
	}
    
    
}
