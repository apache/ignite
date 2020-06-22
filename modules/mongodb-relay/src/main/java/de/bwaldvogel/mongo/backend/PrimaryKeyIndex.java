package de.bwaldvogel.mongo.backend;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.bson.Document;

/**
  *  主键在document里面，不需要创建索引
 * @author WBPC1158
 *
 * @param <P>
 */
public class PrimaryKeyIndex<P> extends AbstractUniqueIndex<P> {
    
    private MongoCollection<P> collection;

    public PrimaryKeyIndex(String name, MongoCollection<P> coll,List<IndexKey> keys, boolean sparse) {
        super(name, keys, sparse);
        this.collection = coll;
    }

    @Override
    public long getCount() {
        return collection.count();
    }

    @Override
    public boolean isEmpty() {
        return collection.isEmpty();
    }

    @Override
    public long getDataSize() {
        return getCount(); // TODO
    }

    @Override
    protected P removeDocument(KeyValue keyValue) {
        return (P)keyValue.get(0);
    }

    @Override
    protected boolean putKeyPosition(KeyValue keyValue, P position) {        
        return true;
    }

    @Override
    protected P getPosition(KeyValue keyValue) {
    	return (P)keyValue.get(0);
    }

    @Override
    protected Iterable<Entry<KeyValue, P>> getIterable(Object o) {
        return new ArrayList<>();
    }
    
    @Override
    public void add(Document document, P position, MongoCollection<P> collection) {
    	
    }
    
    @Override
    protected boolean containsKey(KeyValue keyValue) {
    	AbstractMongoCollection<P> coll = (AbstractMongoCollection<P>) collection;
        return coll.getDocument(getPosition(keyValue)) != null;
    }
}
