package de.bwaldvogel.mongo.backend.memory.index;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;

public class MemoryUniqueIndex extends AbstractUniqueIndex<Integer> {

    private final Map<List<Object>, Integer> index = new ConcurrentHashMap<>();

    public MemoryUniqueIndex(List<IndexKey> keys, boolean sparse) {
        super(keys, sparse);
    }

    @Override
    public long getCount() {
        return index.size();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }

    @Override
    protected Integer removeDocument(List<Object> key) {
        return index.remove(key);
    }

    @Override
    protected boolean containsKey(List<Object> key) {
        return index.containsKey(key);
    }

    @Override
    protected boolean putKeyPosition(List<Object> key, Integer position) {
        Integer oldValue = index.putIfAbsent(key, position);
        return oldValue == null;
    }

    @Override
    protected Integer getPosition(List<Object> key) {
        return index.get(key);
    }

    @Override
    protected Iterable<Entry<List<Object>, Integer>> getIterable() {
        return index.entrySet();
    }

}
