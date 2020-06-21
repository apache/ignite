package de.bwaldvogel.mongo.backend.ignite;


import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.cache.Cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiTuple;

import de.bwaldvogel.mongo.backend.AbstractUniqueIndex;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.KeyValue;
import de.bwaldvogel.mongo.backend.Missing;


public class IgniteUniqueIndex extends AbstractUniqueIndex<Object> {

    private IgniteCache<KeyValue, Object> mvMap;

    IgniteUniqueIndex(IgniteCache<KeyValue, Object> mvMap, String name, List<IndexKey> keys, boolean sparse) {
        super(name, keys, sparse);
        this.mvMap = mvMap;
    }

    @Override
    protected Object removeDocument(KeyValue key) {
        return mvMap.getAndRemove(key);
    }

    @Override
    protected boolean containsKey(KeyValue key) {
        return mvMap.containsKey(key);
    }

    @Override
    protected boolean putKeyPosition(KeyValue key, Object position) {
       return mvMap.putIfAbsent(key, Missing.ofNullable(position));       
    }

    @Override
    protected Iterable<Entry<KeyValue, Object>> getIterable() {
    	
    	ScanQuery<KeyValue, Object> scan = new ScanQuery<>();
    	 
    	QueryCursor<Cache.Entry<KeyValue, Object>>  cursor = mvMap.query(scan);    		
    	    
        return new EntrySet(cursor);
    }

    @Override
    protected Object getPosition(KeyValue key) {
        return mvMap.get(key);
    }

    @Override
    public long getCount() {
        return mvMap.sizeLong(CachePeekMode.PRIMARY);
    }

    @Override
    public boolean isEmpty() {
        return mvMap.size(CachePeekMode.PRIMARY)==0;
    }
    
    @Override
    public long getDataSize() {
        return getCount();
    }

    public class EntrySet implements Iterable<Map.Entry<KeyValue, Object>> {
    	QueryCursor<Cache.Entry<KeyValue, Object>>  cursor;
		EntrySet(QueryCursor<Cache.Entry<KeyValue, Object>>  cursor) {
			this.cursor = cursor;
		}

		@Override
		public Iterator<Map.Entry<KeyValue, Object>> iterator() {
			return new KeyValueIterator(cursor.iterator());
		}
	}

	public static class KeyValueIterator implements Iterator<Map.Entry<KeyValue, Object>> {
		private final Iterator<Cache.Entry<KeyValue, Object>> _iter;

		KeyValueIterator(Iterator<Cache.Entry<KeyValue, Object>> iter) {
			_iter = iter;
		}

		public boolean hasNext() {
			return _iter.hasNext();
		}

		public Map.Entry<KeyValue, Object> next() {
			Cache.Entry<KeyValue, Object> entry = _iter.next();
			return new IgniteBiTuple<KeyValue, Object>(entry.getKey(),entry.getValue()) ;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
