package de.bwaldvogel.mongo.backend.h2;

import java.util.List;
import java.util.Map.Entry;

import org.h2.mvstore.MVMap;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.Missing;

public class H2UniqueIndex extends AbstractUniqueIndex<Object> {

    private MVMap<KeyValue, Object> mvMap;

    H2UniqueIndex(MVMap<KeyValue, Object> mvMap, String name, List<IndexKey> keys, boolean sparse) {
        super(name, keys, sparse);
        this.mvMap = mvMap;
    }

    @Override
    protected Object removeDocument(KeyValue keyValue) {
        return mvMap.remove(keyValue);
    }

    @Override
    protected boolean putKeyPosition(KeyValue keyValue, Object position) {
        Object oldValue = mvMap.putIfAbsent(keyValue, Missing.ofNullable(position));
        return oldValue == null;
    }

    @Override
    protected Iterable<Entry<KeyValue, Object>> getIterable(Object queryObject) {
        return mvMap.entrySet();
    }

    @Override
    protected Object getPosition(KeyValue keyValue) {
        return mvMap.get(keyValue);
    }

    @Override
    public long getCount() {
        return mvMap.sizeAsLong();
    }

    @Override
    public boolean isEmpty() {
        return mvMap.isEmpty();
    }

    @Override
    public long getDataSize() {
        return getCount();
    }

    //add@byron
    public MVMap<KeyValue, Object> getMVMap() {
        return this.mvMap;
    }
}
