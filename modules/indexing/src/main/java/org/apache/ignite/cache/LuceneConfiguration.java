package org.apache.ignite.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.lucene.analysis.Analyzer;

import org.h2.util.Utils;

/**
 * lucene index config for doc type.
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

	// default index analyzer
	private Analyzer indexAnalyzer;

	// default query analyzer
	private Analyzer queryAnalyzer;

	private GridQueryTypeDescriptor type = null;

	public LuceneConfiguration() {

		this.storeTextFieldValue = Utils.getProperty("h2.storeDocumentTextInIndex", false);
		this.storeValue = Utils.getProperty("h2.storeValueInIndex", false);
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

	public GridQueryTypeDescriptor getType() {
		return type;
	}

	public void setType(GridQueryTypeDescriptor type) {
		this.type = type;
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

	public static LuceneConfiguration getInstance(String region) {
		LuceneConfiguration _instance = _instanceMap.get(region);
		if (_instance == null) {
			_instance = new LuceneConfiguration();
			_instanceMap.put(region, _instance);
		}
		return _instance;
	}

	static Map<String, LuceneConfiguration> _instanceMap = new HashMap<>();
}
