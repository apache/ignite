package org.elasticsearch.relay.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.elasticsearch.relay.ESRelay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class BinaryObjectMatch implements IgniteBiPredicate<Object, BinaryObject>{
	private static final long serialVersionUID = 1L;	
	
	private final ObjectNode query;	
	
	public BinaryObjectMatch(ObjectNode query) {		
		this.query = query;		
	}
	
	public static List<String> convertToList(Iterator<String> iterator) {  
		Stream<String> stream = StreamSupport.stream(  
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),  
                false  
        );  
  
        // 使用Collectors.toList()将Stream转换为List  
        List<String> list = stream.collect(Collectors.toList());
        return list;
	}
	

	@Override 
	public boolean apply(Object key, BinaryObject other) {		
     	
		if(other instanceof BinaryObject) {
			if (this.matches((BinaryObject)other, query)) {
	            return true;
	        }
			return false;
		}
     	return true;
     }
	
	protected List<String> splitKey(String key) {
        List<String> keys = List.of(key.split("\\.", -1));
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).isEmpty() && i != keys.size() - 1) {                
                return List.of(key);
            }
        }
        return keys;
    }
	
	/**
	 * 过滤
	 * @param document
	 * @param query
	 * @return
	 */
    public boolean matches(BinaryObject document, ObjectNode query) {
    	List<String> fields = convertToList(query.fieldNames());
        for (String key : fields) {
            Object queryValue = query.get(key);
            //-validateQueryValue(queryValue, key);
            List<String> keys = splitKey(key);
            if(keys.size()!=1) {
            	continue;
            }
            
            if(!document.hasField(key)) {
            	continue;
            }
            Object documentValue = document.field(key);
            if (documentValue instanceof Collection<?>) {
                Collection<?> documentValues = (Collection<?>) documentValue;
                if (queryValue instanceof ObjectNode) {
                	ObjectNode queryDocument = (ObjectNode) queryValue;
                    boolean matches = checkMatchesAnyValue(queryDocument, documentValues);
                    if (matches) {
                        continue;
                    }
                    return false;
                } else if (queryValue instanceof ArrayNode) {
                	boolean matches = checkMatchesValue(queryValue, documentValues);
                	if (matches) {
                        continue;
                    }
                    return false;
                } else if (!checkMatchesAnyValue(queryValue, documentValues)) {
                    return false;
                }
            }
            else if (documentValue instanceof Map) {
            	documentValue = ESRelay.objectMapper.convertValue((Map)documentValue,ObjectNode.class);
            }

            return checkMatchesValue(queryValue, documentValue);
        }

        return true;
    }
    
    private boolean checkMatchesValue(Object queryValue, Object value) {
        if (queryValue instanceof ObjectNode && value instanceof ObjectNode) {
        	ObjectNode queryObject = (ObjectNode) queryValue;
        	ObjectNode valueObject = (ObjectNode) value;
            
            List<String> fields = convertToList(queryObject.fieldNames());
            for (String key : fields) {
                Object querySubvalue = queryObject.get(key);
                Object valueSub = valueObject.get(key);
                if (key.startsWith("$")) {
                    
                } else if (isNullOrMissing(querySubvalue) && !isNullOrMissing(valueSub)) {
                    return false;
                } else if (!checkMatchesValue(querySubvalue, valueSub)) {
                    // the value of the query itself can be a complex query
                    return false;
                }
            }
            return true;
        }        
        else if (queryValue instanceof ArrayNode && value instanceof ArrayNode) {
        	ArrayNode queryObject = (ArrayNode) queryValue;
        	ArrayNode valueObject = (ArrayNode) value;
        	boolean matches = checkMatchesAllValues(queryObject, valueObject);            
            return matches;
        }
        else if (value instanceof ArrayNode) {        	
        	ArrayNode valueObject = (ArrayNode) value;
        	boolean matches = checkMatchesAnyValue(queryValue, valueObject);            
            return matches;
        }
        else if (queryValue instanceof ArrayNode) {        	
        	ArrayNode valueObject = (ArrayNode) queryValue;
        	boolean matches = checkMatchesAnyValue(value, valueObject);            
            return matches;
        }

        return nullAwareEquals(value, queryValue);
    }

    private boolean checkMatchesAllValues(ArrayNode queryValue, ArrayNode values) {        
        
        if (queryValue.isEmpty() || values.isEmpty()) {
            return false;
        }
        Iterator<Entry<String, JsonNode>> it = queryValue.fields();
        while (it.hasNext()) {
        	Entry<String, JsonNode> query = it.next();
            if (!checkMatchesAnyValue(query.getValue(), values)) {
                return false;
            }
        }
        return true;
    }
    
    protected boolean checkMatchesAnyValue(Object queryValue, Collection<?> values) {      

        int i = 0;
        for (Object value : values) {
            if (checkMatchesValue(queryValue, value)) {                
                return true;
            }
            i++;
        }
        return false;
    }
    
    protected boolean checkMatchesAnyValue(Object queryValue, ArrayNode values) {
        int i = 0;
        for (Object value : values) {
            if (checkMatchesValue(queryValue, value)) {                
                return true;
            }
            i++;
        }
        return false;
    }
    
    public static boolean nullAwareEquals(Object a, Object b) {
        if (a == b || a!=null && a.equals(b)) {
            return true;
        } else if (isNullOrMissing(a) && isNullOrMissing(b)) {
            return true;
        } else if (isNullOrMissing(a) || isNullOrMissing(b)) {
            return false;
        } else if (a instanceof byte[] && b instanceof byte[]) {
            byte[] bytesA = (byte[]) a;
            byte[] bytesB = (byte[]) b;
            return Arrays.equals(bytesA, bytesB);
        } else {
            Object normalizedA = normalizeValue(a);
            Object normalizedB = normalizeValue(b);
            return Objects.equals(normalizedA, normalizedB);
        }
    }
    
    public static Object normalizeValue(Object value) {
        if (isNullOrMissing(value)) {
            return null;
        } else if (value instanceof Number) {
            return ((Number) value);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            ObjectNode result = ESRelay.jsonNodeFactory.objectNode();
            for (Entry<String, Object> entry : map.entrySet()) {
                result.putPOJO(entry.getKey(), normalizeValue(entry.getValue()));
            }
            return result;
        } else if (value instanceof Collection<?>) {
            Collection<?> collection = (Collection<?>) value;
            return collection.stream()
                .map(BinaryObjectMatch::normalizeValue)
                .collect(Collectors.toList());
        } else {
            return value;
        }
    }
    
    public static boolean isNullOrMissing(Object value) {
    	if (value==null || value instanceof NullNode || value instanceof MissingNode) {
    		return true;
    	}
    	return false;
    }

}
