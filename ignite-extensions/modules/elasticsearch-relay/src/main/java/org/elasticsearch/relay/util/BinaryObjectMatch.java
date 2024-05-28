package org.elasticsearch.relay.util;

import java.nio.charset.StandardCharsets;
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
        	boolean matched = false;
        	JsonNode queryValue = query.get(key);
            //-validateQueryValue(queryValue, key);
            List<String> keys = splitKey(key);
            if(keys.size()>1) { // field.keyword
            	key = keys.get(0);
            }
            
            if(!document.hasField(key)) {
            	continue;
            }
            Object documentValue = document.field(key);
            if (documentValue instanceof Collection<?>) {
                Collection<?> documentValues = (Collection<?>) documentValue;
                if (queryValue instanceof ObjectNode) {
                	ObjectNode queryDocument = (ObjectNode) queryValue;
                	matched = checkMatchesAnyValue(queryDocument, documentValues);
                    if (matched) {
                        continue;
                    }
                    return false;
                } else if (queryValue instanceof ArrayNode) {
                	matched = checkMatchesAnyValue(queryValue, documentValues);
                	if (matched) {
                        continue;
                    }
                    return false;
                } else if (!checkMatchesAnyValue(queryValue, documentValues)) {
                    return false;
                }
            }
            else if (documentValue instanceof Map) {
            	ObjectNode jsonValue = ESRelay.objectMapper.convertValue((Map)documentValue,ObjectNode.class);
            	matched = checkMatchesValue(queryValue, jsonValue);
                if (matched) {
                    continue;
                }
                return false;
            }
            else if (documentValue instanceof Comparable) {
	            matched = checkMatchesValue(queryValue, documentValue);
	            if (matched) {
	                continue;
	            }
	            else {
	            	return false;
	            }
            }
        }

        return true;
    }
    
    private boolean checkMatchesValue(JsonNode queryValue, Object value) {
        if (value instanceof JsonNode) {        	
        	JsonNode valueObject = (JsonNode) value;            
            return checkMatchesValue(queryValue,valueObject);
        }        
        
        if (queryValue instanceof ArrayNode) {        	
        	ArrayNode querySet = (ArrayNode) queryValue;
        	for (JsonNode term : querySet) {
                if (checkMatchesValue(term, value)) {                
                    return true;
                }               
            }
        	return false;
        }
        else if (queryValue instanceof ObjectNode && value instanceof Comparable) {
        	ObjectNode queryObject = (ObjectNode) queryValue; 
        	String op = "OR";
        	if(queryObject.has("operator")) {
            	op = queryObject.get("operator").asText();
            }
            if(op.equals("OR") && queryObject.has("query")) {
            	return nullAwareEquals(value.toString(), queryObject.get("query"));
            }
            if(op.equals("NOT") && queryObject.has("query")) {
            	return !nullAwareEquals(value.toString(), queryObject.get("query"));
            }
            return true;
        }

        return nullAwareEquals(value, queryValue);
    }
    
    /**
     * 
     * @param queryValue
     * @param value 只可能是object或者标量
     * @return
     */
    private boolean checkMatchesValue(JsonNode queryValue, JsonNode value) {
        if (queryValue instanceof ObjectNode && value instanceof ObjectNode) {
        	ObjectNode queryObject = (ObjectNode) queryValue;
        	ObjectNode valueObject = (ObjectNode) value;
            
            List<String> fields = convertToList(queryObject.fieldNames());
            for (String key : fields) {
            	JsonNode querySubvalue = queryObject.get(key);
            	JsonNode valueSub = valueObject.get(key);
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
        else if (queryValue instanceof ObjectNode && !value.isObject()) {
        	ObjectNode queryObject = (ObjectNode) queryValue; 
        	String op = "OR";
        	if(queryObject.has("operator")) {
            	op = queryObject.get("operator").asText();
            }
            if(op.equals("OR") && queryObject.has("query")) {
            	return nullAwareEquals(value.asText(), queryObject.get("query"));
            }
            if(op.equals("NOT") && queryObject.has("query")) {
            	return !nullAwareEquals(value.asText(), queryObject.get("query"));
            }
            return true;
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
    
    protected boolean checkMatchesAnyValue(JsonNode queryValue, Collection<?> values) {      

        int i = 0;
        for (Object value : values) {
            if (checkMatchesValue(queryValue, value)) {                
                return true;
            }
            i++;
        }
        return false;
    }
    
    protected boolean checkMatchesAnyValue(JsonNode queryValue, ArrayNode values) {
        int i = 0;
        for (JsonNode value : values) {
            if (checkMatchesValue(queryValue, value)) {                
                return true;
            }
            i++;
        }
        return false;
    }
    /**
     * 
     * @param a is value
     * @param b is query
     * @return
     */
    public static boolean nullAwareEquals(Object a, JsonNode b) {
        if (a == b || a!=null && a.equals(b)) {
            return true;
        } else if (isNullOrMissing(a) && isNullOrMissing(b)) {
            return true;
        } else if (isNullOrMissing(a) || isNullOrMissing(b)) {
            return false;
        } else if (a instanceof byte[]) {
            byte[] bytesA = (byte[]) a;
            if(b.isArray()) {
	            ArrayNode bytesB = (ArrayNode) b;
	            if(bytesA.length==bytesB.size()) {
	            	return Arrays.toString(bytesA).equals(bytesB.toString());
	            }
	            return false;
            }
            else {
            	a = new String(bytesA,StandardCharsets.UTF_8);
            	return a.equals(b.toString());
            }
        } else if(b.isNumber()) {
            Object normalizedA = normalizeValue(a);
            Object normalizedB = b;
            return Objects.equals(normalizedA.toString(), normalizedB.toString());
        } else if(b.isTextual()) {
            Object normalizedA = normalizeValue(a);            
            return Objects.equals(normalizedA.toString(), b.asText());
        }
        return false;
    }
    
    public static boolean nullAwareEquals(String a, JsonNode b) {
        if (a!=null && a.equals(b)) {
            return true;
        } else if (isNullOrMissing(a) && isNullOrMissing(b)) {
            return true;
        } else if (isNullOrMissing(a) || isNullOrMissing(b)) {
            return false;       
        } else {            
            return a.equals(b.asText());
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
