package de.bwaldvogel.mongo.backend.h2;

import java.util.List;
import java.util.Map.Entry;

import org.h2.mvstore.MVMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.Missing;

public class H2UniqueIndex extends AbstractUniqueIndex<Object> {

    private MVMap<List<Object>, Object> mvMap;

    H2UniqueIndex(MVMap<List<Object>, Object> mvMap, List<IndexKey> keys, boolean sparse) {
        super(keys, sparse);
        this.mvMap = mvMap;
    }

    @Override
    protected Object removeDocument(List<Object> key) {
        return mvMap.remove(key);
    }

    @Override
    protected boolean containsKey(List<Object> key) {
        return mvMap.containsKey(key);
    }

    @Override
    protected boolean putKeyPosition(List<Object> key, Object position) {
        Object oldValue = mvMap.putIfAbsent(key, Missing.ofNullable(position));
        return oldValue == null;
    }

    @Override
    protected Iterable<Entry<List<Object>, Object>> getIterable() {
        return mvMap.entrySet();
    }

    @Override
    protected Object getPosition(List<Object> key) {
        return mvMap.get(key);
    }

    @Override
    public long getCount() {
        return mvMap.sizeAsLong();
    }

    @Override
    public long getDataSize() {
        return getCount();
    }

}
