package de.bwaldvogel.mongo.backend.ignite.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;

import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;

import org.apache.ignite.internal.processors.igfs.IgfsBaseBlockKey;

import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.lucene.util.BytesRef;

import com.fasterxml.jackson.databind.util.LRUMap;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonEncoder;


public class DocumentUtil {
	
	public static LRUMap<BytesRef,Object> keyDict = new LRUMap<>(100,5000);
	
	/**
	 *  将对象在类里面的公开字段作为key，生成document
	 * @param doc
	 * @param instance
	 * @param cls
	 * @return
	 */
	public static Document objectToDocumentForClass(Document doc, Object instance,Class<?> cls) {		
		//MapFactry x;		
		for(Field field : cls.getDeclaredFields()){
			if(!Modifier.isStatic(field.getModifiers())) {
				try {
	              Object result = null;
	              field.setAccessible(true);
	              result = field.get(instance);
	              if( result != null && !doc.containsKey(field.getName())) {
	            	  if(result instanceof IgniteUuid || result instanceof IgfsBaseBlockKey) {
	            		  result = result.toString();
	            	  }
	            	  try {
	            		  byte t = BsonEncoder.determineType(result);
	            		  doc.append(field.getName(), result); 
	            	  }
	            	  catch(Exception e) {
	            		  doc.append(field.getName(), new Document(result));
	            	  }	            	 
	              }
	             
	            } catch (Exception e) {		             
	                e.printStackTrace();
	            }
			}
		}
		return doc;
	}
	/**
	 * 对非binary对象进行文档化
	 * @param instance
	 * @return
	 */
	public static Document toKeyValuePairs(Object instance) {		
		//MapFactry x;
		Document doc = new Document();
		doc = objectToDocumentForClass(doc,instance,instance.getClass());
		Class<?> p = instance.getClass().getSuperclass();
		while(p!=Object.class) {
			doc = objectToDocumentForClass(doc,instance,p);
			p = p.getSuperclass();
		}
		return doc;			 
	}

	
	public static Document objectToDocument(Object key,Object obj,String idField){
		if(obj instanceof byte[] || obj instanceof Number || obj instanceof CharSequence || obj.getClass().isArray()) {
			key = toDocumentKey(key,idField);
			Document doc = new Document();
			doc.append(idField, key);
			doc.append("_data", obj);
			return doc;
		}
		else if(obj instanceof BinaryObject) {
			BinaryObject bobj = (BinaryObject) obj;
			return binaryObjectToDocument(key,bobj,idField);
		}
		else if(obj instanceof Vector) {
			Vector bobj = (Vector) obj;
			Document doc = new Document();
			doc.append(idField, key);
			doc.append("_data", bobj.getStorage().data());
			doc.append("_meta", new Document(bobj.getMetaStorage()));
			return doc;
		}
		else {
			key = toDocumentKey(key,idField);
			Document doc = new Document();			
			Map<String, Object> kv = toKeyValuePairs(obj);
			doc.putAll(kv);
			doc.append(idField, key);
			return doc;
		}
		
	}
	
	/**
	 * top decode
	 * @param key
	 * @param obj
	 * @param idField document _id
	 * @return
	 */
	public static Document binaryObjectToDocument(Object key, BinaryObject obj, String idField){
		Document doc = null;
		Object $value = binaryObjectToDocument(obj);
		if($value instanceof Document) {
			doc = (Document)$value;
			if(key!=null) {
	    		key = toDocumentKey(key,idField);
	    		doc.append(idField, key);
	    	}
		}
		else {
			doc = objectToDocument(key, $value, idField);
		}
    	
	    return doc;
	}   
    
	/**
	 * document key only support bson type
	 * @param key
	 * @param idField
	 * @return
	 */
	public static Object toDocumentKey(Object key,String idField) {
		if(key!=null) {
    		if(key instanceof BinaryObject){
				BinaryObject $arr = (BinaryObject)key;
				key = binaryObjectToDocument($arr);
				if(key instanceof Map) {
					Map fileds = (Map) key;
					if(fileds.containsKey(idField)) {
						key = $arr.field(idField);
					}
					else if(fileds.containsKey("id")) {
						key = $arr.field("id");
					}					
				}
				byte[] buff = Base64.getEncoder().encode(key.toString().getBytes(StandardCharsets.UTF_8));
				keyDict.put(new BytesRef(buff), $arr);
				return buff;
			}
    	}
	    return key;
	}
	
	public static Object toBinaryKey(Object key) {
		if (key == null) {
            return Missing.getInstance();
        }
		if(key instanceof byte[]){
			Object bKey = keyDict.get(new BytesRef((byte[])key));
			if(bKey!=null) {
				return bKey;
			}
		}
		if(key instanceof Number){
			key = Utils.normalizeNumber((Number)key);
		}
	    return key;
	}

    public static Object binaryObjectToDocument(BinaryObject bobj){
    	Collection<String> fields = bobj.type().fieldNames();
    	try {    		
        	if(fields.size()==0) {
        		return bobj.deserialize();
        	}
    	}
    	catch(Exception e) {
    		e.printStackTrace();    		
    	}
    	
    	Document doc = new Document();
	    for(String field: fields){	    	
	    	String $key =  field;
	    	Object $value = bobj.field(field);
			try {
			
				if($value instanceof List){
					List $arr = (List)$value;
					List<Object> $arr2 = new ArrayList<>($arr.size());
					for(int i=0;i<$arr.size();i++) {
						Object $valueSlice = $arr.get(i);
						if($valueSlice instanceof BinaryObject){
							BinaryObject $arrSlice = (BinaryObject)$valueSlice;					
							$valueSlice = binaryObjectToDocument($arrSlice);
						}	
						$arr2.add($valueSlice);
					}
					$value = ($arr2);
				}
				else if($value instanceof Set){
					Set $arr = (Set)$value;
					Set $arr2 = new HashSet<>($arr.size());
					Iterator it = $arr.iterator();
					while(it.hasNext()) {
						Object $valueSlice = it.next();
						if($valueSlice instanceof BinaryObject){
							BinaryObject $arrSlice = (BinaryObject)$valueSlice;					
							$valueSlice = binaryObjectToDocument($arrSlice);
						}	
						$arr2.add($valueSlice);
					}
					$value = ($arr2);
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					final Document docItem = new Document();
					$arr.forEach((k,v)->{
						if(v instanceof BinaryObject) {
							BinaryObject $arrSlice = (BinaryObject)v;					
							v = binaryObjectToDocument($arrSlice);
						}
						docItem.put(k.toString(),v);
					});
					$value = docItem;
					
				}
				if($value instanceof BinaryObject){
					BinaryObject $arr = (BinaryObject)$value;					
					$value = binaryObjectToDocument($arr);
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
    

    /** {@inheritDoc} */
    public static <F> F readOtherBinaryField(Object buf,int hdr) {
    	if(buf==null) return null;
      
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
            	val = buf.getClass()==Date.class? buf: null;
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
            	val = buf.getClass()==Timestamp.class? buf: null;
            	if(val==null) {
            		long time = Long.parseLong(buf.toString());
            		val = new Timestamp(time);
            	}
                break;
            }

            case GridBinaryMarshaller.TIME: {
            	val = buf.getClass()==Time.class? buf: null;
            	if(val==null) {
            		long time = Long.parseLong(buf.toString());
            		val = new Time(time);
            	} 
                break;
            }

            case GridBinaryMarshaller.UUID: {
            	val = buf.getClass()==UUID.class? buf: UUID.fromString(buf.toString());

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
    
   
    /**
     * 
     * @param igniteBinary
     * @param keyValue
     * @param obj
     * @param typeName 
     * @param keyField BinaryObject id 
     * @return
     */
    public static BinaryObject documentToBinaryObject(IgniteBinary igniteBinary,String typeName, Document doc, String idField){	
    	
    	BinaryTypeImpl type = (BinaryTypeImpl)igniteBinary.type(typeName);
		BinaryObjectBuilder bb = igniteBinary.builder(typeName);		
		Set<Map.Entry<String,Object>> ents = doc.entrySet();
	    for(Map.Entry<String,Object> ent: ents){	    	
	    	String $key =  ent.getKey();
	    	Object $value = ent.getValue();
	    	
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
					$value = $arr.asMap();
				}
				else if($value instanceof Map){
					Map $arr = (Map)$value;
					//$value = new HashMap($arr);
					//$value = ($arr);
				}
				else if(type!=null && $value instanceof Number) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value = readNumberBinaryField((Number)$value,field.typeId());
					}
				}
				else if(type!=null) {
					BinaryFieldMetadata field = type.metadata().fieldsMap().get($key);
					if(field!=null) {
						$value = readOtherBinaryField($value,field.typeId());
					}
				}
				if($key.equals(idField) && doc.size()>1) {
								
				}
				else if($key.equals("_data") && doc.size()<=2) {
					Object bValue = igniteBinary.toBinary($value);
					if(bValue instanceof BinaryObject) {						
						return (BinaryObject)bValue;
					}
				}
				else {
					Object bValue = igniteBinary.toBinary($value);
					bb.setField($key, bValue);
				}
				
				
			} catch (Exception e) {				
				e.printStackTrace();
			}	    	
	    }
	    
	    BinaryObject  bobj = bb.build();
	    return bobj;
	}
}
