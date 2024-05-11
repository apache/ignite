package org.apache.ignite.internal.processors.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.plugin.IgnitePlugin;

import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.IndexKey;
import de.bwaldvogel.mongo.backend.ignite.IgniteBackend;
import de.bwaldvogel.mongo.backend.ignite.IgniteBinaryCollection;
import de.bwaldvogel.mongo.backend.ignite.IgniteDatabase;
import de.bwaldvogel.mongo.backend.ignite.IgniteLuceneIndex;
import de.bwaldvogel.mongo.backend.ignite.IgniteVectorIndex;
import de.bwaldvogel.mongo.bson.Document;

public class MongoPlugin implements IgnitePlugin {
	IgniteBackend backend;
	String databaseName;	
	
	
	public IgniteDatabase database() {
		return (IgniteDatabase)backend.resolveDatabase(databaseName);
	}
	
	public IgniteBinaryCollection collecion(String collectionName) {
		return (IgniteBinaryCollection)this.database().resolveCollection(collectionName, false);
	}
	
	public IgniteVectorIndex createVectorIndex(String collectionName,String field,Document options) {
		IgniteBinaryCollection coll = this.collecion(collectionName);
		String indexName = field+"_knnVector";
		for(Index<Object> index: coll.getIndexes()) {
			if(index.getName().equals(indexName)) {
				return (IgniteVectorIndex)index;
			}
		}
		
		IgniteEx ex = (IgniteEx) database().getIgnite();
		IndexKey key = new IndexKey(field,false,options);
		
		IgniteVectorIndex index = new IgniteVectorIndex(ex.context(),coll,indexName,Arrays.asList(key),false);
		coll.addIndex(index);
		Document indexDesc = new Document();
		indexDesc.append("ns", databaseName+"."+collectionName);
		indexDesc.append("name", indexName);
		indexDesc.append("sparse", false);
		indexDesc.append("key", new Document(field,options));
		collecion("system.indexes").addDocument(indexDesc);
		return index;
	}
	
	public IgniteLuceneIndex createLuceneTextIndex(String collectionName,Document fieldsDesc) {
		IgniteBinaryCollection coll = this.collecion(collectionName);
		String indexName = String.join("_", fieldsDesc.keySet())+"_text";
		for(Index<Object> index: coll.getIndexes()) {
			if(index.getName().equals(indexName)) {
				return (IgniteLuceneIndex)index;
			}
		}
		IgniteEx ex = (IgniteEx) database().getIgnite();
		List<IndexKey> keys = new ArrayList<>();
		for(Map.Entry<String,Object> field: fieldsDesc.entrySet()) {
			if(field.getValue() instanceof Document) {
				IndexKey key = new IndexKey(field.getKey(),false,(Document)field.getValue());
				keys.add(key);
			}
			else {
				IndexKey key = new IndexKey(field.getKey(),field.getValue().toString().equals("1") || field.getValue().toString().equals("true"));
				keys.add(key);
			}
		}
		
		IgniteLuceneIndex index = new IgniteLuceneIndex(ex.context(),coll,indexName, keys, true);
		coll.addIndex(index);
		Document indexDesc = new Document();
		indexDesc.append("ns", databaseName+"."+collectionName);
		indexDesc.append("name", indexName);
		indexDesc.append("sparse", true);
		indexDesc.append("key", fieldsDesc);
		collecion("system.indexes").addDocument(indexDesc);
		return index;
	}

}
