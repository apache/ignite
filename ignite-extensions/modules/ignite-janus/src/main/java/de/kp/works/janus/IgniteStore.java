package de.kp.works.janus;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import de.kp.works.ignite.IgniteClient;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;

public class IgniteStore implements KeyColumnValueStore {

	private final IgniteStoreManager igniteManager;
	private final IgniteClient igniteClient;

	private final String name;	
	private final IgniteCache<String,BinaryObject> cache;
    
	public IgniteStore(IgniteStoreManager manager, final String name, final IgniteClient client) {

		this.igniteManager = manager;
		this.name = name;

		this.cache = client.getOrCreateCache(name);
		this.igniteClient = client;
	
	}
	/**
	 * 
	 *  Remove all entries from the Ignite cache; this is achieved
	 *  by leveraging the 'clear' method of Apache Ignite
	 */
    public void clear() {
    	
		if (this.cache == null) return;
		this.cache.clear();
    		
    }

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public void close() {
	}

    /**
     * This method is responsible for retrieving the list of columns
     * that are actually assigned to a certain row key
     */
	@Override
	public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) {
		return getKeysRangeQuery(query.getKey(), query, txh);
	}
	
	/********** getSlice support **********/
	
	private EntryList getKeysRangeQuery(final StaticBuffer hashKey, final KeySliceQuery query,
            final StoreTransaction txh) {

		IgniteEntryBuilder builder = new IgniteEntryBuilder();
		builder.hashKey(hashKey);

		builder.rangeKeyStart(query.getSliceStart());
		builder.rangeKeyEnd(query.getSliceEnd());
		
		Map<String, IgniteValue> items = builder.build();

		List<Entry> entries = this.igniteClient.getColumnRange(cache, query, items);
		return StaticArrayEntryList.of(entries);

	}
   	
	@Override
	public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) {

		throw new UnsupportedOperationException("[IgniteStore] getSlice based on SliceQuery is not supported.");
	}

	@Override
	public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh)
			throws BackendException {

        mutateOneKey(key, new KCVMutation(additions, deletions), txh);
		
	}

	/** SYNCHRONIZED **/

    private void mutateOneKey(final StaticBuffer key, final KCVMutation mutation, final StoreTransaction txh) throws BackendException {
        igniteManager.mutateMany(Collections.singletonMap(name, Collections.singletonMap(key, mutation)), txh);
    }

	@Override
	public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) {

		/* Do nothing */

	}
	
	@Override
	public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) {
        throw new UnsupportedOperationException("[IgniteStore] getKeys based on KeyRangeQuery is not supported.");
        
	}

	/**
	 * The slice query specifies a column slice (range keys)
	 * and expects to retrieve the list row or hash keys that
	 * fit to this column slice
	 */
	@Override
	public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) {
		/*
		 * The Ignite entry builder is used to transform provided [StaticBuffer] 
		 * range keys (columns) into a [Hex String] representation as this format
		 * can be ordered regularly
		 */
		IgniteEntryBuilder builder = new IgniteEntryBuilder();	

		builder.rangeKeyStart(query.getSliceStart());
		builder.rangeKeyEnd(query.getSliceEnd());
		
		Map<String, IgniteValue> items = builder.build();
		return this.igniteClient.getKeySlice(cache, query, items);

	}

	public void processMutations(final Map<StaticBuffer, KCVMutation> mutationMap, final IgniteStoreTransaction txh) {
 
       for (Map.Entry<StaticBuffer, KCVMutation> entry : mutationMap.entrySet()) {

			final StaticBuffer hashKey = entry.getKey();
            final KCVMutation mutation = entry.getValue();

            /* Filter out deletions that are also added */

            final Set<StaticBuffer> add = mutation.getAdditions().stream()
                .map(Entry::getColumn).collect(Collectors.toSet());

            final List<StaticBuffer> mutableDeletions = mutation.getDeletions().stream()
                .filter(del -> !add.contains(del))
                .collect(Collectors.toList());

            if (mutation.hasAdditions()) {
                processAdditions(hashKey, mutation.getAdditions(), txh);
            }

            if (!mutableDeletions.isEmpty()) {
                processDeletions(hashKey, mutableDeletions, txh);
            }
        }
    }
	    
    private void processAdditions(final StaticBuffer hashKey, final List<Entry> additions, final IgniteStoreTransaction txh) {
    	
    		List<IgniteCacheEntry> entries = additions.stream().map(addition -> {
    			
    			StaticBuffer rangeKey = addition.getColumn();
    			StaticBuffer value  = addition.getValue();
    			
    			final Map<String, IgniteValue> items = new IgniteEntryBuilder()
            			.hashKey(hashKey)
                    .rangeKey(rangeKey)
                    .value(value)
                    .build();
    			
    			
    			return new IgniteCacheEntry(items);
 
    		}).collect(Collectors.toList());
 		igniteClient.putAll(cache, entries);

    }
    
    private void processDeletions(final StaticBuffer hashKey, final List<StaticBuffer> deletions, final IgniteStoreTransaction txh) {
    	
		List<IgniteCacheEntry> entries = deletions.stream().map(rangeKey -> {

			final Map<String, IgniteValue> items = new IgniteEntryBuilder()
        			.hashKey(hashKey)
                .rangeKey(rangeKey)
                .build();
			
			
			return new IgniteCacheEntry(items);

		}).collect(Collectors.toList());

		igniteClient.removeAll(cache, entries);
		
    }
	@Override
	public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
		/*
		 * The Ignite entry builder is used to transform provided [StaticBuffer] 
		 * range keys (columns) into a [Hex String] representation as this format
		 * can be ordered regularly
		 */
		IgniteEntryBuilder builder = new IgniteEntryBuilder();	
		final Map<SliceQuery, KeyIterator> map = new LinkedHashMap<>();
		List<SliceQuery> queriesList = (List)U.field(queries,"queries");
		for(SliceQuery query: queriesList) {	
			builder.rangeKeyStart(query.getSliceStart());
			builder.rangeKeyEnd(query.getSliceEnd());
			
			Map<String, IgniteValue> items = builder.build();
			KeyIterator keyStore = this.igniteClient.getKeySlice(cache, query, items);
			
			map.put(query, keyStore);
		}
		return new IgniteKeySlicesIterator(map);
	}

}
