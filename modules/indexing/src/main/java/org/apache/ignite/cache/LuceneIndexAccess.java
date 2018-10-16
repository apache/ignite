package org.apache.ignite.cache;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneDirectory;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

/**
 * A wrapper for the Lucene writer and searcher.
 */
public class LuceneIndexAccess {   
	 /** */
    private final AtomicLong updateCntr = new GridAtomicLong();   

    /**
     * The index writer.
     */
	public IndexWriter writer;

    /**
     * The index reader.
     */
	public IndexReader reader;

    /**
     * The index searcher.
     */
	public IndexSearcher searcher;
    
	public Set<String> fields = new HashSet<>();
    
	public LuceneConfiguration config;
	
	public LuceneIndexAccess(LuceneConfiguration idxConfig,String path) throws IOException{		 
    	
        this.open(idxConfig, path);     
	}    
	
	public void open(LuceneConfiguration idxConfig,String path) throws IOException{
		this.config = idxConfig;   
    	this.fields.clear();
    	
        Directory indexDir =  null;
              
        if(path.startsWith(FullTextLucene.IN_MEMORY_PREFIX)){
        	indexDir = new RAMDirectory();
        }
        else if(idxConfig.isOffHeapStore()){ // offheap store
        	indexDir = new GridLuceneDirectory(new GridUnsafeMemory(0));
        }
        else{
        	indexDir = FSDirectory.open(new File(path).toPath());
        }                   

        Analyzer analyzer = new StandardAnalyzer();                  
		
		if(config.getIndexAnalyzer()!=null){
			analyzer = config.getIndexAnalyzer();
		}
		
        IndexWriterConfig conf = new IndexWriterConfig(analyzer);
        conf.setCommitOnClose(false); // we by default don't commit on close   
        if(this.writer!=null || config.isPersistenceEnabled()){
        	conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);   
        }else{
        	conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);   
        }
       
       
        IndexWriter writer = new IndexWriter(indexDir, conf);
        //see http://wiki.apache.org/lucene-java/NearRealtimeSearch
        IndexReader reader = DirectoryReader.open(writer);
       
        this.writer = writer;
        this.reader = reader;
        this.searcher = new IndexSearcher(reader);              
	}    
	
	
	public String cacheName(){
		return config.cacheName();
	}
	
	public GridQueryTypeDescriptor type() {
		return config.type();
	}
	
    /**
     * Commit all changes to the Lucene index.
     */
    public void commitIndex() throws IOException {        
        writer.commit();
        // recreate Searcher with the IndexWriter's reader.               
        reader.close();
        reader = DirectoryReader.open(writer);
        searcher = new IndexSearcher(reader);            
    }
    
    public void close() throws IOException{        	
    	 reader.close();
    	 writer.close();
    }
    
    public void increment(){
    	updateCntr.incrementAndGet();
    }
    
    public void flush() throws IgniteCheckedException{
    	  try {
              long updates = updateCntr.get();

              if (updates != 0) {
              	  commitIndex();

                  updateCntr.addAndGet(-updates);
              }
          }
          catch (Exception e) {
              throw new IgniteCheckedException(e);
          }
    }
}
