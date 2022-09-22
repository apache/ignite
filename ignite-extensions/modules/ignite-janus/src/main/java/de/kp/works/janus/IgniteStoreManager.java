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

import com.google.common.base.Preconditions;
import de.kp.works.ignite.IgniteClient;
import de.kp.works.ignite.IgniteContext;
import org.apache.ignite.Ignite;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData.Container;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IgniteStoreManager implements KeyColumnValueStoreManager {
	/*
	 * The features that specify this ignite store; they are used
	 * by JanusGraph during initialization
	 */
    private final StoreFeatures features;
    /*
     * Reference to Apache Ignite that is transferred to the key value
     * store to enable cache operations
     */
    private final Ignite ignite = IgniteContext.getInstance().getIgnite();
    private final IgniteClient database = new IgniteClient(ignite);

    /*
     * JanusGraph leverages multiple stores to manage and persist
     * graph data
     */
    private final ConcurrentHashMap<String, IgniteStore> stores;

	public IgniteStoreManager(final Configuration configuration) {
		
        /*
         * Initialize minimal store features; JanusGraph comes with
         * a wide range of additional features, so this is the place
         * where to introduce customized features
         */
		features = new StandardStoreFeatures.Builder()
				.keyConsistent(configuration)
				.persists(true)
				/*
				 * If this flag is set to `false`, JanusGraph with do this
				 * via [ExpectedValueCheckingStoreManager], which is less
				 * effective
				 */
				.locking(true)
				.optimisticLocking(true)
				/*
				 * This indicates that we do not support key range queries;
				 * see Ignite key value store
				 */
                .keyOrdered(false)
				.distributed(true)
				.multiQuery(true)
				.batchMutation(true)
				.localKeyPartition(false)
				/*
				 * Unordered scan also specify that key range queries are
				 * not supported (see ignite key value store)
				 */
                .orderedScan(false)
                .unorderedScan(true)
			 	.build();
		
		/*
		 * Initialize stores
		 */
		stores = new ConcurrentHashMap<>();	

	}

	public StoreTransaction beginTransaction(BaseTransactionConfig config) {
		return new IgniteStoreTransaction(config);
	}

	public void close() {

		for (IgniteStore store : stores.values()) {
            store.close();
        }

		stores.clear();
		
	}

	public void clearStorage() {

		for (IgniteStore store : stores.values()) {
            store.clear();
        }

        stores.clear();
		
	}

	public boolean exists() {
		return !stores.isEmpty();
	}

	public StoreFeatures getFeatures() {
		return features;
	}

	public String getName() {
		return toString();
	}

	public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException(" Get local key partition.");
	}

	public IgniteStore openDatabase(String name) {
		return getStore(name);
	}

	@Override
	public IgniteStore openDatabase(String name, Container metaData) {
		return getStore(name);
	}

	@Override
	public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) {
        
		/*
		 * We divide mutation operations by the affected key value store
		 * and also distinguish between create or update (additions) and
		 * delete (deletions) operations
		 */
		final IgniteStoreTransaction tx = (IgniteStoreTransaction)txh;
         for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> mutationMapEntry : mutations.entrySet()) {

        	 	final IgniteStore store = openDatabase(mutationMapEntry.getKey());
            final Map<StaticBuffer, KCVMutation> storeMutations = mutationMapEntry.getValue();

            store.processMutations(storeMutations, tx);
      	
        }

	}

	private IgniteStore getStore(String name) {

		if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new IgniteStore(this, name, database));
        }

		IgniteStore store = stores.get(name);

		Preconditions.checkNotNull(store);
        return store;
		
	}
}
