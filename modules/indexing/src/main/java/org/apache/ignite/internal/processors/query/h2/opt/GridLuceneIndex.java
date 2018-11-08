/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2.opt;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.FullTextLucene;
import org.apache.ignite.cache.FullTextQueryIndex;
import org.apache.ignite.cache.LuceneConfiguration;
import org.apache.ignite.cache.LuceneIndexAccess;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryIndexDescriptor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.h2.util.JdbcUtils;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.UrlResource;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;

import static org.apache.ignite.internal.processors.query.QueryUtils.KEY_FIELD_NAME;
import static org.apache.ignite.internal.processors.query.QueryUtils.VAL_FIELD_NAME;



/**
 * Lucene fulltext index.
 */
public class GridLuceneIndex implements AutoCloseable {
    /** Field name for string representation of value. */
    public static final String VAL_STR_FIELD_NAME = "_gg_val_str__";
    public static final String DLF_LUCENE_CONFIG = "default";
    
    
    static ApplicationContext springCtx = null;

    /** */
    private final String cacheName;

    /** */
    private final GridQueryTypeDescriptor type;  

    /** */
    private final String[] idxdFields;   

    /** */
    private final GridKernalContext ctx;    
    
    private LuceneConfiguration config;
    
    private FullTextQueryIndex textIdx;
    
    private LuceneIndexAccess indexAccess;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param type Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public GridLuceneIndex(GridKernalContext ctx, @Nullable String cacheName, GridQueryTypeDescriptor type)
        throws IgniteCheckedException {
        this.ctx = ctx;
        this.cacheName = cacheName;
        this.type = type;       
        
        try{
        	if(springCtx==null)
        		springCtx = initContext(this.getClass().getResourceAsStream("/lucene.xml"));    
        	
    		if(springCtx.containsBean(cacheName)){
    			this.config = springCtx.getBean(cacheName,LuceneConfiguration.class);
    		}
    		else if(springCtx.containsBean(DLF_LUCENE_CONFIG)){
    			this.config = springCtx.getBean(DLF_LUCENE_CONFIG,LuceneConfiguration.class);
    		}
    		else{
    			this.config = LuceneConfiguration.getConfiguration(type.schemaName(),type.tableName());     
    		}
    	}
    	catch(Exception e){   
    		this.config = LuceneConfiguration.getConfiguration(type.schemaName(),type.tableName());  
    		ctx.grid().log().error(e.getMessage(),e);
    	}
        
        
        //if no ctx use lucene to store val.
        if(ctx==null){
        	this.config.setStoreValue(true);
        }
        else{
        	FullTextLucene.ctx = ctx;    
        	this.config.cacheName(cacheName);
        	this.config.type(type);        	
        	this.config.setPersistenceEnabled(ctx.config().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled());
        }  
        //store config
        LuceneConfiguration.putConfiguration(type.schemaName(),type.tableName(),this.config);
        
        QueryIndex qtextIdx = ((QueryIndexDescriptorImpl)type.textIndex()).getQueryIndex();  
        if(qtextIdx instanceof FullTextQueryIndex){
        	try{        		
        		this.textIdx = (FullTextQueryIndex)qtextIdx;
        		if(this.textIdx.getAnalyzer()!=null)
        			this.config.setIndexAnalyzer(springCtx.getBean(this.textIdx.getAnalyzer(),Analyzer.class));
        		if(this.textIdx.getQueryAnalyzer()!=null)
        			this.config.setQueryAnalyzer(springCtx.getBean(this.textIdx.getQueryAnalyzer(),Analyzer.class));
        	}
        	catch(BeansException e){
        		e.printStackTrace();
        		ctx.grid().log().error(e.getMessage(),e);
        	}
        }         

        try {
        	 indexAccess = FullTextLucene.getIndexAccess(null, type.schemaName(), type.tableName());        	 
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        GridQueryIndexDescriptor idx = type.textIndex();

        if (idx != null) {
            Collection<String> fields = indexAccess.fields;

            idxdFields = new String[fields.size() + 1];

            fields.toArray(idxdFields);
        }
        else {
            assert type.valueTextIndex() || type.valueClass() == String.class;

            idxdFields = new String[1];
        }

        idxdFields[idxdFields.length - 1] = VAL_STR_FIELD_NAME;
    }
    
    /**
     * @param stream Input stream containing Spring XML configuration.
     * @return Context.
     * @throws IgniteCheckedException In case of error.
     */
    private ApplicationContext initContext(InputStream stream) throws IgniteCheckedException {
        GenericApplicationContext springCtx;

        try {
            springCtx = new GenericApplicationContext();

            XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(springCtx);

            reader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD);

            reader.loadBeanDefinitions(new InputStreamResource(stream));

            springCtx.refresh();
        }
        catch (BeansException e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context " +
                    "(make sure all classes used in Spring configuration are present at CLASSPATH) ", e);
            else
                throw new IgniteCheckedException("Failed to instantiate Spring XML application context" +
                    ", err=" + e.getMessage() + ']', e);
        }

        return springCtx;
    }
      

	public Analyzer getQueryAnalyzer(LuceneConfiguration config){
		if(config.getQueryAnalyzer()!=null){
			return config.getQueryAnalyzer();
		}		
		Analyzer ana = new StandardAnalyzer();
		return ana;
	}
	

    /**
     * @return Cache object context.
     */
    private CacheObjectContext objectContext() {
        if (ctx == null)
            return null;

        return ctx.cache().internalCache(cacheName).context().cacheObjectContext();
    }

    /**
     * Stores given data in this fulltext index.
     *
     * @param k Key.
     * @param v Value.
     * @param ver Version.
     * @param expires Expiration time.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void store(CacheObject k, CacheObject v, GridCacheVersion ver, long expires) throws IgniteCheckedException {
        CacheObjectContext coctx = objectContext();
        
        Field.Store storeText = config.isStoreTextFieldValue()?  Field.Store.YES : Field.Store.NO;

        Object key = k.isPlatformType() ? k.value(coctx, false) : k;
        Object val = v.isPlatformType() ? v.value(coctx, false) : v;

        Document doc = new Document();

        boolean stringsFound = false;
                
        if (type.valueTextIndex() || type.valueClass() == String.class) {
        	if(config.isStoreValue()){
        		doc.add(new TextField(VAL_STR_FIELD_NAME, val.toString(), Field.Store.YES));
        	}
        	else{
        		doc.add(new TextField(VAL_STR_FIELD_NAME, val.toString(), Field.Store.NO));
        	}
            stringsFound = true;
        }        

        for (int i = 0, last = idxdFields.length - 1; i < last; i++) {
            Object fieldVal = type.value(idxdFields[i], key, val);

            if (fieldVal != null) {
            	if(fieldVal.getClass().isArray()){
            		if(fieldVal instanceof String[]){
                		String[] terms = (String[])fieldVal;
                		for(int j=0;j<terms.length;j++){
                			if(terms[j]!=null)
                				doc.add(new TextField(idxdFields[i], terms[j], storeText));    
                		}
                	}
                	else if(fieldVal instanceof Object[]){
                		Object[] terms = (Object[])fieldVal;
                		for(int j=0;j<terms.length;j++){
                			if(terms[j]!=null)
                				doc.add(new TextField(idxdFields[i], terms[j].toString(), storeText));    
                		}
                	}
            	}
            	else{
            		doc.add(new TextField(idxdFields[i], fieldVal.toString(), storeText));
            	}

                stringsFound = true;
            }
        }

        BytesRef keyByteRef = new BytesRef(k.valueBytes(coctx));

        try {
            final Term term = new Term(KEY_FIELD_NAME, keyByteRef);

            if (!stringsFound) {
            	indexAccess.writer.deleteDocuments(term);

                return; // We did not find any strings to be indexed, will not store data at all.
            }

            doc.add(new StringField(KEY_FIELD_NAME, keyByteRef, Field.Store.YES));
            doc.add(new StringField(FullTextLucene.FIELD_TABLE, this.type.name(), Field.Store.YES));
            //add@byron may not store value
            if(config.isStoreValue()){
            	
            	if (type.valueClass() != String.class)
            		doc.add(new StoredField(VAL_FIELD_NAME, v.valueBytes(coctx)));
            }
            //end@

            doc.add(new StoredField(QueryUtils.VER_FIELD_NAME, ver.toString()));

            doc.add(new LongPoint(FullTextLucene.EXPIRATION_TIME_FIELD_NAME, expires));

            // Next implies remove than add atomically operation.
            indexAccess.writer.updateDocument(term, doc);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
        	indexAccess.increment();
        }
    }

    /**
     * Removes entry for given key from this index.
     *
     * @param key Key.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(CacheObject key) throws IgniteCheckedException {
        try {
        	indexAccess.writer.deleteDocuments(new Term(KEY_FIELD_NAME,
                new BytesRef(key.valueBytes(objectContext()))));
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
        	indexAccess.increment();
        }
    }

    /**
     * Runs lucene fulltext query over this index.
     *
     * @param qry Query.
     * @param filters Filters over result.
     * @return Query result.
     * @throws IgniteCheckedException If failed.
     */
    public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> query(String qry, IndexingQueryFilter filters) throws IgniteCheckedException {
        try {
        	indexAccess.flush();
        }
        catch (Exception e) {
            throw new IgniteCheckedException(e);
        }

        IndexSearcher searcher;

        TopDocs docs;

        try {
            searcher = indexAccess.searcher;

            MultiFieldQueryParser parser = new MultiFieldQueryParser(idxdFields, getQueryAnalyzer(this.config));

//            parser.setAllowLeadingWildcard(true);
            String [] items = qry.split("\\s");
            //qty: hello limit:100 type:blog user:xiaoming
            int limit =  600; // Integer.MAX_VALUE;
            String uid = null;
            String sort = null;
            StringBuilder sb = new StringBuilder();
            for(String item:items){
            	if(item.startsWith("limit:")){
            		limit = Integer.parseInt(item.substring("limit:".length()));
            	}
            	else if(item.startsWith("sort:")){
            		sort = item.substring("sort:".length());
            	}
            	else if(item.startsWith("user:")){
            		uid = item.substring("user:".length());
            	}
            	else{
            		sb.append(item);
            		sb.append(' ');
            	}
            }

            // Filter expired items.
            Query filter = LongPoint.newRangeQuery(FullTextLucene.EXPIRATION_TIME_FIELD_NAME, U.currentTimeMillis(), Long.MAX_VALUE);

            BooleanQuery.Builder query = new BooleanQuery.Builder()
                .add(parser.parse(qry), BooleanClause.Occur.MUST)
                .add(filter, BooleanClause.Occur.FILTER);
            
            if(uid!=null){
            	query.add(new TermQuery(new Term("user",uid)),BooleanClause.Occur.MUST);
            }                

            if(sort!=null){
            	String[] sorts = sort.split(",");
            	Sort sortObj = new Sort();
            	SortField[] sf = new SortField[sorts.length];
            	for(int j=0;j<sorts.length;j++){            		
            		sf[j] = new SortField(sorts[j],SortField.Type.STRING,true);
            	}
            	sortObj.setSort(sf);
            	docs = searcher.search(query.build(), limit, sortObj);
            }
            else{
            	docs = searcher.search(query.build(), limit);
            }
        }
        catch (Exception e) {
            //U.closeQuiet(indexAccess.reader);

            throw new IgniteCheckedException(e);
        }

        IndexingQueryCacheFilter fltr = null;

        if (filters != null)
            fltr = filters.forCache(cacheName);

        return new It<K,V>(indexAccess.reader, searcher, docs.scoreDocs, fltr);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(indexAccess.writer);
        U.close(indexAccess.writer.getDirectory(), ctx.log(GridLuceneIndex.class));
    }

    /**
     * Key-value iterator over fulltext search result.
     */   
    private class It<K, V> extends GridCloseableIteratorAdapter<IgniteBiTuple<K, V> > {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final IndexReader reader;

        /** */
        private final IndexSearcher searcher;

        /** */
        private final ScoreDoc[] docs;

        /** */
        private final IndexingQueryCacheFilter filters;        
      
        /** */
        private int idx;

        /** */
        private IgniteBiTuple<K, V> curr;

        /** */
        private CacheObjectContext coctx;

        /**
         * Constructor.
         *
         * @param reader Reader.
         * @param searcher Searcher.
         * @param docs Docs.
         * @param filters Filters over result.
         * @throws IgniteCheckedException if failed.
         */
        private It(IndexReader reader, IndexSearcher searcher, ScoreDoc[] docs, IndexingQueryCacheFilter filters)
                throws IgniteCheckedException {
                this.reader = reader;
                this.searcher = searcher;
                this.docs = docs;
                this.filters = filters;

                coctx = objectContext();

                findNext();
            }

        /**
         * @param bytes Bytes.
         * @param ldr Class loader.
         * @return Object.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private <Z> Z unmarshall(byte[] bytes, ClassLoader ldr) throws IgniteCheckedException {
            if (coctx == null) // For tests.
                return (Z)JdbcUtils.deserialize(bytes, null);

            return (Z)coctx.kernalContext().cacheObjects().unmarshal(coctx, bytes, ldr);
        }

        /**
         * Finds next element.
         *
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("unchecked")
        private void findNext() throws IgniteCheckedException {
            curr = null;
            ClassLoader ldr = null;
            
            GridCacheAdapter cache = null;
            if (ctx != null){
            	cache = ctx.cache().internalCache(cacheName);
            }
            if (ctx != null && ctx.deploy().enabled())
                ldr = cache.context().deploy().globalLoader();
            
            while (idx < docs.length) {
                Document doc;

                try {
                    doc = searcher.doc(docs[idx++].doc);
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                K k = unmarshall(doc.getBinaryValue(KEY_FIELD_NAME).bytes, ldr);

                if (filters != null && !filters.apply(k))
                    continue;
                
                V v = null;
                //add@byron
                if(config.isStoreValue()){
                	v = type.valueClass() == String.class ?
                    (V)doc.get(VAL_STR_FIELD_NAME) :
                    this.<V>unmarshall(doc.getBinaryValue(VAL_FIELD_NAME).bytes, ldr);
                    
                }               
                else{
                	v = (V)cache.get(k,false,false);
                }
                assert v != null;             
                
                //end@
                curr = new IgniteBiTuple<>(k, v);

                break;
            }
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<K, V> onNext() throws IgniteCheckedException {
            IgniteBiTuple<K, V> res = curr;

            findNext();

            return res;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return curr != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            //- U.closeQuiet(reader);
        }
    }
}
