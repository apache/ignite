package org.apache.ignite.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneDirectory;
import org.apache.ignite.internal.processors.query.h2.opt.GridLuceneIndex;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.InputStreamResource;

/**
 * A wrapper for the Lucene writer and searcher.
 */
public class LuceneIndexAccess {   
	public static final String DLF_LUCENE_CONFIG = "default";
	 
    /** spring ctx for lucene.xml */
	public static ApplicationContext springCtx = null;
    
    private static final Map<String, LuceneIndexAccess> INDEX_ACCESS = new ConcurrentHashMap<>();
    
    private static long lastCommitTime = System.currentTimeMillis();

	 /** */
    private final AtomicLong updateCntr = new GridAtomicLong();
    
    /** 基于字段的分析器 */
    public PerFieldAnalyzerWrapper analyzerWrapper;
    
    
    private Directory indexDir =  null;

    /**
     * The index writer.
     */
	public IndexWriter writer;

    /**
     * The index reader.
     */
	public DirectoryReader reader;

    /**
     * The index searcher.
     */
	public IndexSearcher searcher;   
	
	/**
	 *  current cache fields that must indexed by lucene. 
	 */
	public HashMap<String,Map<String,FieldType>> typeFields = new HashMap<>();
	
	public LuceneConfiguration config;
	
	private GridKernalContext ctx;
	/**
	 *  如果config/lucene.xml含有cacheName的bean，则使用这个bean作为luncene配置信息，否则使用default。
	 * @param ctx
	 * @param cacheName
	 * @param path
	 * @throws IOException
	 */
	public LuceneIndexAccess(GridKernalContext ctx, String cacheName,String path) throws IOException{	
		this.ctx = ctx;
		try{
        	if(springCtx==null && ctx!=null)
        		springCtx = initContext(new FileInputStream(ctx.config().getIgniteHome()+"/config/lucene.xml"));    
        	
    		if(springCtx.containsBean(cacheName)){
    			this.config = springCtx.getBean(cacheName,LuceneConfiguration.class);
    		}
    		else if(springCtx.containsBean(DLF_LUCENE_CONFIG)){
    			this.config = new LuceneConfiguration(springCtx.getBean(DLF_LUCENE_CONFIG,LuceneConfiguration.class));
    		}
    		else {
    			this.config = new LuceneConfiguration();
    		}
    		
    	}
    	catch(Exception e){   
    		this.config = new LuceneConfiguration();
    		ctx.grid().log().error(e.getMessage(),e);
    	}
        
        //if no ctx use lucene to store val.
        if(ctx==null){
        	this.config.setStoreValue(true);
        	this.config.cacheName(cacheName);
        }
        else{
        	FullTextLucene.ctx = ctx;    
        	this.config.cacheName(cacheName);        	   	
        	this.config.setPersistenceEnabled(ctx.config().getDataStorageConfiguration().getDefaultDataRegionConfiguration().isPersistenceEnabled());
        } 
    	
        this.open(path);     
	}  
	
	public Map<String,FieldType> fields(String type) {
		Map<String,FieldType> fields = typeFields.get(type);
		if(fields==null){
			fields = new HashMap<String,FieldType>();
			typeFields.put(type,fields);
		}
		return fields;
	}
	
	public Analyzer getFieldAnalyzer(String field) {
		String region = field;
		Analyzer _instance = config.getFieldAnalyzerMap().get(region);
		if (_instance == null) {
			if(config.getQueryAnalyzer()!=null){
				return config.getQueryAnalyzer();
			}		
			Analyzer ana = new StandardAnalyzer();
			return ana;			
		}
		return _instance;
	}	
	
	
	public Map<String,FieldType> init(GridQueryTypeDescriptor type) {
		
		Map<String,FieldType> fields = fields(type.name());
		
		try{        		
			QueryIndex qtextIdx = ((QueryIndexDescriptorImpl)type.textIndex()).getQueryIndex();  
			
            if(qtextIdx instanceof FullTextQueryIndex){
            	FullTextQueryIndex textIdx = (FullTextQueryIndex)qtextIdx;
           	 	
           	   if(textIdx!=null && textIdx.getAnalyzer()!=null)
     			   this.config.setIndexAnalyzer(springCtx.getBean(textIdx.getAnalyzer(),Analyzer.class));
           	   if(textIdx!=null && textIdx.getQueryAnalyzer()!=null)
     			   this.config.setQueryAnalyzer(springCtx.getBean(textIdx.getQueryAnalyzer(),Analyzer.class));
     		
            }
    		  
            Field.Store storeText = config.isStoreTextFieldValue()?  Field.Store.YES : Field.Store.NO;     
        	if(qtextIdx!=null){
        		//-fields.clear();
        		for(String field: type.textIndex().fields()) {
        			Class fieldType = type.fields().get(field);
        			if(config.isStoreTextFieldValue()) {
        				fields.put(field,TextField.TYPE_STORED);
        			}
        			else {
        				fields.put(field,TextField.TYPE_NOT_STORED);
        			}    			
        		}
            }        	
        	
    	}
    	catch(BeansException e){
    		e.printStackTrace();
    		ctx.grid().log().error(e.getMessage(),e);
    	}
		
		return fields;
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
	
	public void open(String path) throws IOException{        
        if(path.startsWith(FullTextLucene.IN_MEMORY_PREFIX)){
        	indexDir = new GridLuceneDirectory(new GridUnsafeMemory(0));
        }
        else if(config.isOffHeapStore()){ // offheap store
        	indexDir = new GridLuceneDirectory(new GridUnsafeMemory(0));
        }
        else{
        	indexDir = FSDirectory.open(new File(path).toPath());
        }

        Analyzer analyzer = null;                 
		
		if(config.getIndexAnalyzer()!=null){
			analyzer = config.getIndexAnalyzer();
		}
		else {
			analyzer = new StandardAnalyzer();
		}		
		analyzerWrapper = new PerFieldAnalyzerWrapper(analyzer, config.getFieldAnalyzerMap());
		
        IndexWriterConfig conf = new IndexWriterConfig(analyzerWrapper);
        conf.setCommitOnClose(config.isPersistenceEnabled()); // we by default don't commit on close   
        if(this.writer!=null || config.isPersistenceEnabled()){
        	conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);   
        }else{
        	conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);   
        }    
       
        IndexWriter writer = new IndexWriter(indexDir, conf);
        //see http://wiki.apache.org/lucene-java/NearRealtimeSearch
        DirectoryReader reader = DirectoryReader.open(writer);
       
        this.writer = writer;
        this.reader = reader;
        this.searcher = new IndexSearcher(reader);           
    }    
	
	
	public String cacheName(){
		return config.cacheName();
	}	
	
    /**
     * Commit all changes to the Lucene index.
     */
    public void commitIndex(boolean applyAllDeletes) throws IOException {
    	writer.commit();
        try {
             // recreate Searcher with the IndexWriter's reader.        	
        	
        	DirectoryReader readerNew = DirectoryReader.openIfChanged(reader, writer, applyAllDeletes);
        	if(readerNew!=null) {
        		this.reader = readerNew;
        		searcher = new IndexSearcher(this.reader);
        	}
        	else {
        		reader = DirectoryReader.open(writer);
        		searcher = new IndexSearcher(reader);  
        	}
        }
        catch(IOException e) {
            //see http://wiki.apache.org/lucene-java/NearRealtimeSearch        	
            reader = DirectoryReader.open(writer);
            searcher = new IndexSearcher(reader);      	
        }
    }
    
    public void close() throws IOException{      
    	 searcher = null;
    	 U.closeQuiet(reader);
    	 U.closeQuiet(writer);
    	 U.close(writer.getDirectory(), ctx.log(GridLuceneIndex.class));
    }
    
    public void increment(){
    	updateCntr.incrementAndGet();
    }
    
    public void flush() throws IOException{
    	long updates = updateCntr.get();

        if (updates != 0) {
      	  updateCntr.addAndGet(-updates);
      	  long current = System.currentTimeMillis();
      	  if(updates>=1024 || current-lastCommitTime>=60*1000) {
      		  commitIndex(true); 
      		  lastCommitTime = current;
      	  }
      	  else {
      		  commitIndex(false);   
      	  }
        }
    }
    

    /**
     * Get the path of the Lucene index for this database.
     *
     * @param conn the database connection
     * @return the path
     */
    protected static String getIndexPath(GridKernalContext ctx, String cacheName){    	
		String subFolder;
		try {
			subFolder = ctx.pdsFolderResolver().resolveFolders().folderName();
		} catch (IgniteCheckedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			subFolder = ctx.localNodeId().toString();
		}
		String path = ctx.config().getWorkDirectory()+File.separator+"lucene"+
					File.separator+subFolder+File.separator+"cache-"+cacheName;
    	return path;
    }

    /**
     * Get the index writer/searcher wrapper for the given connection.
     *
     * @param conn the connection
     * @return the index access wrapper
     */
    public static LuceneIndexAccess getIndexAccess(GridKernalContext ctx, String cacheName)
            throws IOException {
    	String path = getIndexPath(ctx,cacheName);
        synchronized (INDEX_ACCESS) {
            LuceneIndexAccess access = INDEX_ACCESS.get(path);
           
            if (access == null) {
                try {                	
                	access = new LuceneIndexAccess(ctx,cacheName,path);                     
                    
                } catch (IOException e) {
                    throw e;
                }
                
                INDEX_ACCESS.put(path, access);                
                
            }
            
            if (!access.writer.isOpen()) {
                try {
                	access.open(path);                 
                    
                } catch (IOException e) {
                    throw e;
                }               
            }
            
            return access;
        }
    }
    
    /**
     * Close the index writer and searcher and remove them from the index access
     * set.
     *
     * @param access the index writer/searcher wrapper
     * @param indexPath the index path
     */
    public static void removeIndexAccess(LuceneIndexAccess access) {
        synchronized (INDEX_ACCESS) {
            try {
                INDEX_ACCESS.remove(access.writer.getDirectory().toString());  
                access.close();
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
    }
    
    
}
