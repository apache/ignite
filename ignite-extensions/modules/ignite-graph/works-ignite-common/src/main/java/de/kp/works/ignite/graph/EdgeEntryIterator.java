package de.kp.works.ignite.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import de.kp.works.ignite.ValueType;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.IgniteEdgeEntry;

public class EdgeEntryIterator implements CloseableIterator<IgniteEdgeEntry>,AutoCloseable {
	private Iterator<List<?>> sqlResult;
	private FieldsQueryCursor cursor;
	
	public EdgeEntryIterator(FieldsQueryCursor<List<?>> cursor) {
		this.cursor = cursor;
		this.sqlResult = cursor.iterator();
	}
	
	public static List<IgniteEdgeEntry> toList(Iterator<IgniteEdgeEntry> it) {
		ArrayList<IgniteEdgeEntry> list = new ArrayList<IgniteEdgeEntry>();
		while(it.hasNext()) {
			list.add(it.next());
		}
		return list;
	}
	
	public <T> CloseableIterator<T> map(Function<IgniteEdgeEntry,T> func){
		return new CloseableIterator<T>() {			
			@Override
			public boolean hasNext() {				
				return sqlResult.hasNext();
			}

			@Override
			public T next() {
				T newObj = func.apply(EdgeEntryIterator.this.next());
				return newObj;
			}
			
			@Override
		    public void close() {
				EdgeEntryIterator.this.close();
		    }
			
		};
	}
	
	public Stream<IgniteEdgeEntry> stream(){
		return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        this,
                        Spliterator.ORDERED)
                , false);
	}
	
	@Override
    public void close() {
		if(cursor!=null) {
			cursor.close();
			cursor = null;
		}
    }
	 
	@Override
	public boolean hasNext() {		
		return sqlResult.hasNext();
	}
	
	 /*
     * 0 : Cache key
     *
     * 1 : IgniteConstants.ID_COL_NAME (String)
     * 2 : IgniteConstants.ID_TYPE_COL_NAME (String)
     * 3 : IgniteConstants.LABEL_COL_NAME (String)
     * 4 : IgniteConstants.TO_COL_NAME (String)
     * 5 : IgniteConstants.TO_TYPE_COL_NAME (String)
     * 6 : IgniteConstants.FROM_COL_NAME (String)
     * 7 : IgniteConstants.FROM_TYPE_COL_NAME (String)
     * 8 : IgniteConstants.CREATED_AT_COL_NAME (Long)
     * 9 : IgniteConstants.UPDATED_AT_COL_NAME (Long)
     * 10: IgniteConstants.PROPERTY_KEY_COL_NAME (String)
     * 11: IgniteConstants.PROPERTY_TYPE_COL_NAME (String)
     * 12: IgniteConstants.PROPERTY_VALUE_COL_NAME (String)
     */
	@Override
	public IgniteEdgeEntry next() {
		List<?> result = sqlResult.next();
		
		String cacheKey = (String)result.get(0);

        String id     = (String)result.get(1);
        String idType = (String)result.get(2);
        String label  = (String)result.get(3);

        String toId     = (String)result.get(4);
        String toIdType = (String)result.get(5);

        String fromId     = (String)result.get(6);
        String fromIdType = (String)result.get(7);

        Long createdAt  = (Long)result.get(8);
        Long updatedAt  = (Long)result.get(9);

        String propKey   = (String)result.get(10);
        String propType  = (String)result.get(11);
        Object propValue = result.get(12);
        if(!propType.equals("STRING") && !propType.equals("*") && propValue instanceof String) {
        	propValue = ValueUtils.parseValue(propValue.toString(), ValueType.valueOf(propType));
        }

        return new IgniteEdgeEntry(
                cacheKey,
                id,
                idType,
                label,
                toId,
                toIdType,
                fromId,
                fromIdType,
                createdAt,
                updatedAt,
                propKey,
                propType,
                propValue);
		
	}
}
