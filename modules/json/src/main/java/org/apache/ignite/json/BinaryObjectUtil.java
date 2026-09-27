package org.apache.ignite.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;



public class BinaryObjectUtil {
	
	public static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
	
	public static ObjectNode getObjectNode(List<?> row, List<GridQueryFieldMetadata> fieldsMeta) throws Exception {
		// check if there is a query
		ObjectNode jsonQuery = new ObjectNode(jsonNodeFactory);
		int index=0;
		for(GridQueryFieldMetadata meta: fieldsMeta) {
			Object node = row.get(index);					
			JsonNode jsonNode = toJsonValue(node,0);
			jsonQuery.set(meta.fieldName(), jsonNode);			
			index++;
		}
		return jsonQuery;
	}
	
	public static BinaryObject mapToBinaryObject(Ignite ignite, String typeName, Map<String, Object> result){
		JsonNode obj = toJsonValue(result,0);
		return jsonToBinaryObject(ignite,typeName,(ObjectNode)obj);
	}
	
	public static BinaryObject jsonToBinaryObject(Ignite ignite, String typeName, ObjectNode obj){	
		BinaryObjectBuilder bb = ignite.binary().builder(typeName);
		Iterator<Map.Entry<String,JsonNode>> ents = obj.fields();
	    while(ents.hasNext()){	    
	    	Map.Entry<String,JsonNode> ent = ents.next();
	    	String $key =  ent.getKey();
	    	JsonNode $value = ent.getValue();
			try {
			
				if($value.isContainerNode()){
					Object bValue = jsonToObject($value,0);
					bValue = ignite.binary().toBinary(bValue);
					bb.setField($key, bValue);
				}
				else if($value.isMissingNode() || $value.isNull()){
					bb.setField($key, null);
				}
				else if($value.isPojo()){
					Object bValue = ignite.binary().toBinary(((POJONode)$value).getPojo());
					bb.setField($key, bValue);
				}
				else {					
					Object bValue = jsonToObject($value,0);
					bb.setField($key, bValue);
				}
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	    	
	    }
	    return bb.build();
	}
	
	public static Object jsonNodeToObject(JsonNode json){
		return jsonToObject(json,0);
	}
	
	protected static Object jsonToObject(JsonNode json,int depth){
		Object ret = null;
		depth++;
		if(json.isObject()) {
			Map<String,Object> obj = new HashMap<>(json.size());
			Iterator<Map.Entry<String,JsonNode>> ents = json.fields();
			
		    while(ents.hasNext()){	    
		    	Map.Entry<String,JsonNode> ent = ents.next();
		    	String $key =  ent.getKey();
		    	JsonNode $value = ent.getValue();
		    	if(depth<=16) {
		    		Object bValue = jsonToObject($value,depth);
		    		obj.put($key, bValue); 	
		    	}
		    	else {
		    		obj.put($key, $value);
		    	}
		    }
		   
		    ret = obj;
		}
		else if(json.isArray()) {
			ArrayNode array = (ArrayNode) json;
			List<Object> obj = new ArrayList<>(json.size());
			for(int i=0;i<array.size();i++) {
				obj.add(jsonToObject(array.get(i),depth));
			}
			ret = array;
		}
		else if(json.isPojo()) {
			ret = ((POJONode)json).getPojo();
		}
		else if(json.isMissingNode() || json.isNull()) {
			
		}
		else if(json.isNumber()) {
			NumericNode number = (NumericNode) json;
			ret = number.numberValue();
		}
		else if(json.isTextual()) {
			ret = json.asText();
		}
		else if(json.isBinary()) {
			ret = ((BinaryNode)json).binaryValue();
		}
		else if(json.isBoolean()) {
			ret = json.asBoolean();
		}
		else {
			ret = json;
		}
		depth--;		
		return ret;
	}
	
	
	/**
	 * Binary decoder value only support bson type
	 * @param node
	 * @param level
	 * @return
	 */
	public static JsonNode toJsonValue(Object node,int level) {
		if(node!=null) {			
			Object $value = node;
			if(node.getClass()==String.class) {
				return jsonNodeFactory.textNode(node.toString());
			}
			else if(node.getClass()==byte[].class) {
				return jsonNodeFactory.binaryNode((byte[])node);
			}
			else if(node instanceof Number){
				if(BigDecimal.class==node.getClass()) {
					return jsonNodeFactory.numberNode((BigDecimal)node);
				}
				else if(BigInteger.class==node.getClass()) {
					return jsonNodeFactory.numberNode((BigInteger)node);
				}
				else if(Long.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Long)node);
				}
				else if(Double.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Double)node);
				}
				else if(Float.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Float)node);
				}
				else if(Integer.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Integer)node);
				}
				else if(Short.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Short)node);
				}
				else if(Byte.class==node.getClass()) {
					return jsonNodeFactory.numberNode((Byte)node);
				}
				return jsonNodeFactory.textNode(node.toString());
			}
			else if(Boolean.class==node.getClass()) {
				return jsonNodeFactory.booleanNode((Boolean)node);
			}
			else if($value instanceof CharSequence || $value instanceof UUID ){
				return jsonNodeFactory.textNode($value.toString());
			}
			else if($value.getClass().isEnum()) {
				return jsonNodeFactory.textNode(((Enum)$value).name());
			}
			else if($value.getClass().isArray()) {
				if(level>5) {
					return jsonNodeFactory.pojoNode(node);
				}
				if($value instanceof Object[]) {
					Object [] arr = (Object[])$value;
					ArrayNode $arr2 = jsonNodeFactory.arrayNode(arr.length);
					
					for(int i=0;i< arr.length;i++) {							
						$arr2.add(toJsonValue(arr[i],level+1));
					}
					return $arr2;
				}
				if($value instanceof char[]) {
					char[]arr = (char[])$value;
					ArrayNode $arr2 = jsonNodeFactory.arrayNode(arr.length);
					
					for(int i=0;i< arr.length;i++) {							
						$arr2.add(toJsonValue(arr[i],level+1));
					}
					return $arr2;
				}
				if($value instanceof float[]) {
					float[]arr = (float[])$value;
					ArrayNode $arr2 = jsonNodeFactory.arrayNode(arr.length);
					
					for(int i=0;i< arr.length;i++) {							
						$arr2.add(toJsonValue(arr[i],level+1));
					}
					return $arr2;
				}
				if($value instanceof double[]) {
					double[]arr = (double[])$value;
					ArrayNode $arr2 = jsonNodeFactory.arrayNode(arr.length);
					
					for(int i=0;i< arr.length;i++) {							
						$arr2.add(toJsonValue(arr[i],level+1));
					}
					return $arr2;
				}
				if($value instanceof int[]) {
					int[]arr = (int[])$value;
					ArrayNode $arr2 = jsonNodeFactory.arrayNode(arr.length);
					
					for(int i=0;i< arr.length;i++) {							
						$arr2.add(toJsonValue(arr[i],level+1));
					}
					return $arr2;
				}				
			}			
			else if($value instanceof List){
				if(level>5) {
					return jsonNodeFactory.pojoNode(node);
				}
				List $arr = (List)$value;
				ArrayNode $arr2 = jsonNodeFactory.arrayNode($arr.size());
				for(int i=0;i<$arr.size();i++) {
					Object $valueSlice = $arr.get(i);
					
					$arr2.add(toJsonValue($valueSlice,level+1));
				}
				return $arr2;
			}
			else if($value instanceof Set){
				if(level>5) {
					return jsonNodeFactory.pojoNode(node);
				}
				Set $arr = (Set)$value;
				ArrayNode $arr2 = jsonNodeFactory.arrayNode($arr.size());
				Iterator<Object> it = $arr.iterator();
				while(it.hasNext()) {
					Object $valueSlice = it.next();
					$arr2.add(toJsonValue($valueSlice,level+1));
				}
				return $arr2;
			}
			else if($value instanceof Map){
				if(level>5) {
					return jsonNodeFactory.pojoNode(node);
				}
				Map<Object, Object> dict = (Map)$value;
				final ObjectNode doc = jsonNodeFactory.objectNode();
				for(Map.Entry<Object, Object> ent: dict.entrySet()) {
					Object v = ent.getValue();
					doc.set(ent.getKey().toString(), toJsonValue(v,level+1));
				}					
				return doc;
			}			
			else if($value instanceof BinaryObject){
				BinaryObject $arrSlice = (BinaryObject)$value;					
				$value = binaryObjectToJsonNode($arrSlice,level);
			}
			else if(node instanceof JsonNode){
				return (JsonNode)node;
			}
			else {								
				POJONode pnode = new POJONode(node);
				return pnode;
			}
					
    	}
	    return null;
	}


	/**
	 * top decode
	 * @param key
	 * @param obj
	 * @param idField document _id
	 * @return
	 */
	public static JsonNode binaryObjectToJsonNode(BinaryObject obj){
		JsonNode doc = binaryObjectToJsonNode(obj,0);		
	    return doc;
	}
	
    public static JsonNode binaryObjectToJsonNode(BinaryObject bobj,int level){
    	Collection<String> fields = null;
    	try {    		
    		if(bobj instanceof BinaryObjectImpl) {
	    		BinaryObjectImpl bin = (BinaryObjectImpl)bobj;
	    		if(!bin.hasSchema()) {
	    			return toJsonValue(bin.deserialize(),level);
	    		}
    		}
    		else if(bobj instanceof BinaryArray) {
    			BinaryArray bin = (BinaryArray)bobj;
    			return toJsonValue(bin.deserialize(),level);
    		}
    		else if(bobj instanceof BinaryEnumObjectImpl) {
    			BinaryEnumObjectImpl bin = (BinaryEnumObjectImpl)bobj;	    		
    			return toJsonValue(bin.deserialize(),level);
    		}
    		
    		String typeName = bobj.type().typeName();
    		if(typeName.equals("Document") || typeName.equals("SerializationProxy")) {
    			return toJsonValue(bobj.deserialize(),level);
    		}
    		
    		fields = bobj.type().fieldNames();
    		if(fields==null || fields.size()<=1) {
    			return toJsonValue(bobj.deserialize(),level);
    		}    		
    	}
    	catch(BinaryObjectException e) {
    		if(bobj instanceof BinaryEnumObjectImpl) {
    			BinaryEnumObjectImpl bin = (BinaryEnumObjectImpl)bobj;
	    		return toJsonValue(bin.enumOrdinal(),level);
    		}
    		fields = bobj.type().fieldNames();	
    	}
    	
    	ObjectNode doc = jsonNodeFactory.objectNode();
    	
	    for(String field: fields){	    	
	    	String $key =  field;
	    	Object $value = bobj.field(field);
			try {				
				if($value!=null) {					
					doc.set($key, toJsonValue($value,level+1));
				}
				else {
					doc.set($key, null);
				}
				
			} catch (Exception e) {				
				e.printStackTrace();
			}	    	
	    }
	    return doc;
	}
}
