package de.bwaldvogel.mongo.backend.ignite;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.binary.BinaryFieldEx;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryByteBufferInputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.DuplicateKeyError;
import io.netty.util.internal.StringUtil;

public class IgniteBinaryCollection extends AbstractMongoCollection<Object> {

    private final IgniteCache<Object, BinaryObject> dataMap;
    
    public IgniteBinaryCollection(IgniteDatabase database, String collectionName, String idField, IgniteCache<Object, BinaryObject> dataMap) {
        super(database, collectionName, idField);
        this.dataMap = dataMap;
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        
    }

    @Override
    protected int getDataSize() {
    	int size = (int)dataMap.metrics().getCacheSize();
        return size;
    }


    @Override
    protected Object addDocumentInternal(Document document) {
        final Object key;
        if (idField != null) {
            key = Utils.getSubdocumentValue(document, idField);
        } else {
            key = UUID.randomUUID();
        }
        
        T2<Object,BinaryObject> obj = this.documentToBinaryObject(key,document);
       
        boolean rv = dataMap.putIfAbsent(Missing.ofNullable(obj.getKey()), obj.getValue());
        if(!rv) {
        	throw new DuplicateKeyError(this.getCollectionName(),"Document with key '" + key + "' already existed");
        }
        //Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
        return key;
    }

    @Override
    public int count() {
        return dataMap.size();
    }

    @Override
    protected Document getDocument(Object position) {
    	BinaryObject obj = dataMap.get(position);
    	if(obj==null) return null;
    	return this.binaryObjectToDocument(position,obj,idField);
    }

    @Override
    protected void removeDocument(Object position) {
        boolean remove = dataMap.remove(position);
        if (!remove) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Object findDocumentPosition(Document document) {
    	 Object key = document.getOrDefault(this.idField, null);
    	 if(key!=null) {
    		 return key;
    	 }    	 
         return null;
    }



    @Override
    protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip,
            int numberToReturn) {
        List<Document> matchedDocuments = new ArrayList<>();
        
        ScanQuery<Object, BinaryObject> scan = new ScanQuery<>(
        		 new IgniteBiPredicate<Object, BinaryObject>() { 	                
					private static final long serialVersionUID = 1L;

					@Override public boolean apply(Object key, BinaryObject other) {
 	                	Document document = binaryObjectToDocument(key,other,idField);
 	                	if (documentMatchesQuery(document, query)) {
 	                       return true;
 	                    }
 	                	return false;
 	                }
 	            }
        	
        );
	 
		QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
		//Iterator<Cache.Entry<Object, BinaryObject>> it = cursor.iterator();
	    for (Cache.Entry<Object, BinaryObject> entry: cursor) {	 	    	
	    	Document document = this.binaryObjectToDocument(entry.getKey(),entry.getValue(),this.idField);
	    	if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
	    }

	    sortDocumentsInMemory(matchedDocuments, orderBy);
        return applySkipAndLimit(matchedDocuments, numberToSkip, numberToReturn);
    }

    @Override
    protected void handleUpdate(Object key, Document oldDocument,Document document) {
        // noop   	
    	T2<Object,BinaryObject> obj = this.documentToBinaryObject(key,document);
        
        boolean rv = dataMap.putIfAbsent(Missing.ofNullable(obj.getKey()), obj.getValue());        
    }


    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
    	// Get the data streamer reference and stream data.
    	//try (IgniteDataStreamer<Object, Document> stmr = Ignition.ignite().dataStreamer(dataMap.getName())) {    
    	//	stmr.receiver(StreamVisitor.from((key, val) -> {}));
    	//}
    	
    	 ScanQuery<Object, BinaryObject> scan = new ScanQuery<>();
    		 
    	 QueryCursor<Cache.Entry<Object, BinaryObject>>  cursor = dataMap.query(scan);
    	//Iterator<Cache.Entry<Object, Document>> it = cursor.iterator();
    	 return StreamSupport.stream(cursor.spliterator(),false).map(entry -> new DocumentWithPosition<>(binaryObjectToDocument(entry.getKey(),entry.getValue(),this.idField), entry.getKey()));		
         
    }
    
    public T2<Object,BinaryObject> documentToBinaryObject(Object keyValue,Document obj){	
    	IgniteBinary igniteBinary = ((IgniteDatabase) this.database).getIgnite().binary();
    	
    	String typeName = (String)obj.get("_class");    	
    	String keyField = "id";
    	CacheConfiguration cfg = dataMap.getConfiguration(CacheConfiguration.class);
    	if(!cfg.getQueryEntities().isEmpty()) {
    		Iterator<QueryEntity> qeit = cfg.getQueryEntities().iterator();
    		QueryEntity entity = qeit.next();   		
    		keyField = entity.getKeyFieldName();
    		if(StringUtil.isNullOrEmpty(typeName)) {
        		typeName = entity.getValueType();
        	}	
    	}
    	if(StringUtil.isNullOrEmpty(typeName)) {
    		typeName = dataMap.getName();
    	}		
    	BinaryTypeImpl type = (BinaryTypeImpl)igniteBinary.type(typeName);
		BinaryObjectBuilder bb = igniteBinary.builder(typeName);
		
		Set<Map.Entry<String,Object>> ents = obj.entrySet();
	    for(Map.Entry<String,Object> ent: ents){	    	
	    	String $key =  ent.getKey();
	    	Object $value = ent.getValue();
	    	if($key.equals(this.idField)) {
	    		$key = keyField;
	    	}
			try {
			
				if($value instanceof List){
					List $arr = (List)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Set){
					Set $arr = (Set)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Document){
					Document $arr = (Document)$value;
					//-$value = new HashMap<String,Object>($arr);
					$value = ($arr.asMap());
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					//$value = new HashMap($arr);
					//$value = ($arr);
				}
				else if(type!=null && $value instanceof Number) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value=readNumberBinaryField((Number)$value,field.typeId());
					}
				}
				else if(type!=null) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value=readOtherBinaryField($value,field.typeId());
					}
				}
				if($key==keyField) {
					keyValue = $value;
				}
				
				Object bValue = igniteBinary.toBinary($value);
				bb.setField($key, bValue);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }	   
	    BinaryObject  bobj = bb.build();
	    return new T2(keyValue,bobj);
	}
    
    public static Document binaryObjectToDocument(Object key,BinaryObject obj,String idField){	    	
    	Document doc = new Document();	
    	if(key!=null) {
    		if(key instanceof BinaryObject){
				BinaryObject $arr = (BinaryObject)key;					
				key = $arr.deserialize();
			}	
    		doc.append(idField, key);
    	}
	    for(String field: obj.type().fieldNames()){	    	
	    	String $key =  field;
	    	Object $value = obj.field(field);
			try {
			
				if($value instanceof List){
					List $arr = (List)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Set){
					Set $arr = (Set)$value;
					//-$value = $arr.toArray();
					$value = ($arr);
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					//-$value = new HashMap<String,Object>($arr);
					$value = new Document($arr);
				}
				if($value instanceof BinaryObject){
					BinaryObject $arr = (BinaryObject)$value;					
					$value = binaryObjectToDocument(null,$arr,idField);
				}	
				if($value!=null) {
					doc.append($key, $value);
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return doc;
	}    


    /** {@inheritDoc} */
    public static <F> F readNumberBinaryField(Number buf,int hdr) {
    	if(buf==null) return null;
        try {

            Object val = buf;

            switch (hdr) {
                case GridBinaryMarshaller.INT:
                    val = buf.getClass()==Integer.class? buf: buf.intValue();
                    break;

                case GridBinaryMarshaller.LONG:
                	 val = buf.getClass()==Long.class? buf: buf.longValue();

                    break;               

                case GridBinaryMarshaller.SHORT:
                	val = buf.getClass()==Short.class? buf: buf.shortValue();

                    break;

                case GridBinaryMarshaller.BYTE:
                	val = buf.getClass()==Byte.class? buf: buf.byteValue();

                    break;

               

                case GridBinaryMarshaller.FLOAT:
                	val = buf.getClass()==Float.class? buf: buf.floatValue();

                    break;

                case GridBinaryMarshaller.DOUBLE:
                	val = buf.getClass()==Double.class? buf: buf.doubleValue();

                    break;

                
                case GridBinaryMarshaller.DECIMAL: {
                	val = buf.getClass()==BigDecimal.class? buf: new BigDecimal(buf.toString());
                    break;
                }

                case GridBinaryMarshaller.NULL:
                    val = null;

                    break;               
            }

            return (F)val;
        }
        finally {
           
        }
    }
    

    /** {@inheritDoc} */
    public static <F> F readOtherBinaryField(Object buf,int hdr) {
    	if(buf==null) return null;
        try {

            Object val = buf;;

            switch (hdr) {
                

                case GridBinaryMarshaller.BOOLEAN:
                	 val = buf.getClass()==Boolean.class? buf: Utils.isTrue(buf);

                    break;

                

                case GridBinaryMarshaller.CHAR:
                	val = buf.getClass()==Character.class? buf: buf.toString().charAt(0);

                    break;

               

                case GridBinaryMarshaller.STRING: {
                	val = buf.toString();

                    break;
                }

                case GridBinaryMarshaller.DATE: {
                	val = buf.getClass()==Date.class? buf:null;
                	if(val==null) {
                		DateFormat dateFormatSecond = new SimpleDateFormat("yyyy-mm-dd hh:ss");
                		DateFormat dateFormatDay = new SimpleDateFormat("yyyy-mm-dd");
                		try {
                			val = dateFormatSecond.parse(buf.toString());
                		}catch(ParseException e) {
                			try {
                				val = dateFormatDay.parse(buf.toString());
                			}catch(ParseException e2) {
                				long time = Long.parseLong(buf.toString());
                        		val = new Date(time);
                			}
                		}
                	}

                    break;
                }

                case GridBinaryMarshaller.TIMESTAMP: {
                	
                	val = buf.getClass()==Timestamp.class? buf:null;
                	if(val==null) {
                		long time = Long.parseLong(buf.toString());
                		val = new Timestamp(time);
                	}
                    break;
                }

                case GridBinaryMarshaller.TIME: {
                	val = buf.getClass()==Time.class? buf:null;
                	if(val==null) {
                		long time = Long.parseLong(buf.toString());
                		val = new Time(time);
                	} 
                    break;
                }

                case GridBinaryMarshaller.UUID: {
                	val = buf.getClass()==UUID.class? buf:UUID.fromString(buf.toString());

                    break;
                }


                case GridBinaryMarshaller.NULL:
                    val = null;

                    break;

                default:
                    // Restore buffer position.
                    break;
            }

            return (F)val;
        }
        finally {
          
        }
    }
    
   
}
