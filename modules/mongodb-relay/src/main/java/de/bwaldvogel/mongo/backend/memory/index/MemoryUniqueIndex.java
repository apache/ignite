package de.bwaldvogel.mongo.backend.memory.index;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;

public class MemoryUniqueIndex extends AbstractUniqueIndex<Integer> {

    private final Map<KeyValue, Integer> index = new ConcurrentHashMap<>();

    public MemoryUniqueIndex(String name, List<IndexKey> keys, boolean sparse) {
        super(name, keys, sparse);
    }

    @Override
    public long getCount() {
        return index.size();
    }

    @Override
    public boolean isEmpty() {
        return index.isEmpty();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }

    @Override
    protected Integer removeDocument(KeyValue keyValue) {
        return index.remove(keyValue);
    }

    @Override
    protected boolean putKeyPosition(KeyValue keyValue, Integer position) {
        Integer oldValue = index.putIfAbsent(keyValue, position);
        return oldValue == null;
    }

    @Override
    protected Integer getPosition(KeyValue keyValue) {
        return index.get(keyValue);
    }

    @Override
    protected Iterable<Entry<KeyValue, Integer>> getIterable() {
        return index.entrySet();
    }

}
