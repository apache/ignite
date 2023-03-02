package de.kp.works.ignite;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import de.kp.works.ignite.query.*;

import de.kp.works.ignite.graph.*;

public final class IgniteResultTransform {
	public static final IgniteResultTransform MODULE$ = new IgniteResultTransform();
	
	public static class CloseableEdgeIterator implements CloseableIterator<IgniteResult>, AutoCloseable  {
		private IgniteEdgeEntry last = null;
		final EdgeEntryIterator entries;
		
		public CloseableEdgeIterator(final EdgeEntryIterator entries) {
			this.entries = entries;
		}

		@Override
		public boolean hasNext() {				
			return last!=null || entries.hasNext();
		}
		
		@Override
	    public void close() {
			entries.close();
	    }

		@Override
		public IgniteResult next() {
			IgniteResult igniteResult = new IgniteResult();

			IgniteEdgeEntry head = last;
			if(head==null) head = entries.next();

			/*
			 * Extract common fields
			 */

			String id = head.id;

			String idType = head.idType;
			String label = head.label;

			String toId = head.toId;
			String toIdType = head.toIdType;

			String fromId = head.fromId;
			String fromIdType = head.fromIdType;

			Long createdAt = head.createdAt;
			Long updatedAt = head.updatedAt;

			/*
			 * Add common fields
			 */

			igniteResult.addColumn(IgniteConstants.ID_COL_NAME, idType, id);

			igniteResult.addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label);

			igniteResult.addColumn(IgniteConstants.TO_COL_NAME, toIdType, toId);

			igniteResult.addColumn(IgniteConstants.FROM_COL_NAME, fromIdType, fromId);

			igniteResult.addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt);

			igniteResult.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt);
			
			/*
			 * Extract & add properties
			 */
			last = null;
			igniteResult.addColumn(head.propKey, head.propType, head.propValue);
			while(entries.hasNext()) {
				IgniteEdgeEntry value = entries.next();
				if(value.id.equals(head.id)) {
					igniteResult.addColumn(value.propKey, value.propType, value.propValue);
				}
				else {
					last = value;
					break;
				}
			};
			
			return igniteResult;
		}
		
	

	};	

	
	public static class CloseableVertexIterator implements CloseableIterator<IgniteResult>, AutoCloseable  {
		private IgniteVertexEntry last = null;
		final VertexEntryIterator entries;
		
		public CloseableVertexIterator(final VertexEntryIterator entries) {
			this.entries = entries;
		}
		
		@Override
		public boolean hasNext() {				
			return last!=null || entries.hasNext();
		}
		
		@Override
	    public void close() {
			entries.close();
	    }
		
		@Override
		public IgniteResult next() {
			IgniteResult igniteResult = new IgniteResult();

			IgniteVertexEntry head = last;
			if(head==null) head = entries.next();

			/*
			 * Extract common fields
			 */

			String id = head.id;

			String idType = head.idType;
			String label = head.label;

			Long createdAt = head.createdAt;
			Long updatedAt = head.updatedAt;

			/*
			 * Add common fields
			 */
			igniteResult.addColumn(IgniteConstants.ID_COL_NAME, idType, id);

			igniteResult.addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label);

			igniteResult.addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt);

			igniteResult.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt);
			/*
			 * Extract & add properties
			 */
			last = null;
			igniteResult.addColumn(head.propKey, head.propType, head.propValue);
			while(entries.hasNext()) {
				IgniteVertexEntry value = entries.next();
				if(value.id.equals(head.id)) {
					igniteResult.addColumn(value.propKey, value.propType, value.propValue);
				}
				else {
					last = value;
					break;
				}
			};
			
			return igniteResult;
		}
		
	};



	public static Iterator<IgniteResult> transformEdgeEntries(final EdgeEntryIterator entries) {
		
		return new CloseableEdgeIterator(entries);		
	}
	
	public static Iterator<IgniteResult> transformVertexEntries(final VertexEntryIterator entries) {
		
		return new CloseableVertexIterator(entries);
	}

	public static <E> Iterator<E> map(final Iterator<IgniteResult> iterator, Function<IgniteResult,E> function){
		 return new CloseableIterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }

            @Override
            public void close() {
                CloseableIterator.closeIterator(iterator);
            }
        };
	}
}
