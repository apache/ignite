package de.bwaldvogel.mongo.backend.ignite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.util.typedef.T2;

import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
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

    private IgniteCache<P, BinaryObject> collection;

    public PrimaryKeyIndex(String name, IgniteCache<P, BinaryObject> coll,List<IndexKey> keys, boolean sparse) {
        super(name, keys, sparse);
        this.collection = coll;
    }

    @Override
    public long getCount() {
        return collection.size();
    }

    @Override
    public boolean isEmpty() {
        return collection.size()==0;
    }

    @Override
    public long getDataSize() {
        return collection.size()*1024; // TODO
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
    protected Iterable<Entry<KeyValue, P>> getIterable(Object queriedValue) {    	
    	if(queriedValue!=null) {
    		BinaryObject v = collection.get((P)queriedValue);
    		if(v==null) {
    			return new ArrayList<>();
    		}
    		Entry<KeyValue, P> e = new T2<>(KeyValue.valueOf(queriedValue),(P)queriedValue);
    		return List.of(e);
    	}
    	Iterator<Entry<KeyValue, P>> it = mapIt(collection.iterator());
		 return new Iterable<Entry<KeyValue, P>>() {
	         @Override
	         public Iterator<Entry<KeyValue, P>> iterator()
	         {
	             return it;
	         }
	     };
    }
    
    private Iterator<Entry<KeyValue, P>> mapIt(Iterator<Cache.Entry<P,BinaryObject>> sqlResult){
		return new Iterator<Entry<KeyValue, P>>() {			
			@Override
			public boolean hasNext() {				
				return sqlResult.hasNext();
			}

			@Override
			public Entry<KeyValue, P> next() {
				Cache.Entry<P,BinaryObject> row = sqlResult.next();
				return new T2<>(KeyValue.valueOf(row.getKey()),row.getKey());
			}
		};
	}

    @Override
    public void add(Document document, P position, MongoCollection<P> collection) {

    }

    @Override
    protected boolean containsKey(KeyValue keyValue) {    	
        return collection.containsKey(getPosition(keyValue));
    }
}
