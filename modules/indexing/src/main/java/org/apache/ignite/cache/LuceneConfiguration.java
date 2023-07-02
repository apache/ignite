package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.spring.IgniteSpringHelper;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.lucene.analysis.Analyzer;
import org.h2.util.Utils;

/**
 * lucene index config for doc type. 
 * one indexes per type. multiple type per cache will gen multiple indexes.
 * 
 * @author Hunteron-cp
 *
 */
public class LuceneConfiguration implements PluginConfiguration {
	boolean offHeapStore = false; // 是否使用offheap存储索引

	boolean storeValue = false; // 存储_val
	/**
	 * Whether the text content should be stored in the Lucene index.
	 */
	boolean storeTextFieldValue = false; // 存储索引字段
	
	boolean persistenceEnabled = true; // cache是否开启了持久化

	

	// default index analyzer
	private Analyzer indexAnalyzer;

	// default query analyzer
	private Analyzer queryAnalyzer;
	
		
	private String cacheName = null;
	
	

	public LuceneConfiguration() {

		this.storeTextFieldValue = Utils.getProperty("h2.storeDocumentTextInIndex", false);
		this.storeValue = Utils.getProperty("h2.storeValueInIndex", false);		
	}

	public boolean isPersistenceEnabled() {
		return persistenceEnabled;
	}

	public void setPersistenceEnabled(boolean persistenceEnabled) {
		this.persistenceEnabled = persistenceEnabled;
	}
	
	public Analyzer getIndexAnalyzer() {
		return indexAnalyzer;
	}

	public void setIndexAnalyzer(Analyzer indexAnalyzer) {		
		this.indexAnalyzer = indexAnalyzer;
	}

	public Analyzer getQueryAnalyzer() {
		return queryAnalyzer;
	}

	public void setQueryAnalyzer(Analyzer queryAnalyzer) {
		this.queryAnalyzer = queryAnalyzer;
	}	

	public boolean isStoreTextFieldValue() {
		return storeTextFieldValue;
	}

	public void setStoreTextFieldValue(boolean storeTextFieldValue) {
		this.storeTextFieldValue = storeTextFieldValue;
	}

	public boolean isOffHeapStore() {
		return offHeapStore;
	}

	public void setOffHeapStore(boolean memeryStore) {
		this.offHeapStore = memeryStore;
	}

	public boolean isStoreValue() {
		return storeValue;
	}

	public void setStoreValue(boolean storeValue) {
		this.storeValue = storeValue;
	}

	public LuceneConfiguration cacheName(String cacheName){
		this.cacheName = cacheName;
		return this;
	}
	
	public String cacheName(){
		return cacheName;
	}		
	
	public Analyzer getFieldAnalyzer(String schema,String field) {
		String region = QueryUtils.createTableCacheName(schema.toUpperCase(),field.toUpperCase());
		Analyzer _instance = _fieldAnalyzerMap.get(region);
		if (_instance == null) {
			return this.getIndexAnalyzer();
		}
		return _instance;
	}
	
	public boolean setFieldAnalyzer(String schema,String field,Analyzer config) {
		String region = QueryUtils.createTableCacheName(schema.toUpperCase(),field.toUpperCase());
		Analyzer _instance = _fieldAnalyzerMap.get(region);
		if (_instance == null) {			
			_fieldAnalyzerMap.put(region, config);
			return true;
		}
		return false;
	}

	private Map<String, Analyzer> _fieldAnalyzerMap = new HashMap<>();
}
