package org.elasticsearch.relay.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;


public class ESUtil {
	private static ObjectNode getFilterObject(ESQuery query) throws Exception {
		// check if there is a query
		ObjectNode jsonQuery = query.getQuery();
		if (jsonQuery == null) {
			jsonQuery = new ObjectNode(ESRelay.jsonNodeFactory);
			query.setQuery(jsonQuery);
		}

		// check if there is a query sub-object
		ObjectNode queryObj = (ObjectNode)jsonQuery.get(ESConstants.Q_QUERY);
		if (queryObj == null) {
			queryObj = new ObjectNode(ESRelay.jsonNodeFactory);
			jsonQuery.put(ESConstants.Q_QUERY, queryObj);
		}

		// check if there is a filtered sub-object
		ObjectNode filteredObj = (ObjectNode)queryObj.get(ESConstants.Q_FILTERED);
		if (filteredObj == null) {
			filteredObj = new ObjectNode(ESRelay.jsonNodeFactory);
			queryObj.put(ESConstants.Q_FILTERED, filteredObj);
		}

		// check if there is a filter sub-object
		ObjectNode filterObj = (ObjectNode)filteredObj.get(ESConstants.Q_FILTER);
		if (filterObj == null) {
			filterObj = new ObjectNode(ESRelay.jsonNodeFactory);
			filteredObj.put(ESConstants.Q_FILTER, filterObj);
		}

		return filterObj;
	}

	public static ArrayNode getOrCreateFilterArray(ESQuery query) throws Exception {
		ObjectNode filterObj = getFilterObject(query);

		// actual array of filters
		// check if there is a logical 'and' array
		ArrayNode andArray = filterObj.withArray(ESConstants.Q_AND);
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
			filterObj.set(ESConstants.Q_AND, andArray);
		}

		return andArray;
	}

	public static void replaceFilterArray(ESQuery query, ArrayNode andArray) throws Exception {
		ObjectNode filterObj = getFilterObject(query);

		// remove existing array
		filterObj.remove(ESConstants.Q_AND);

		// replace with given or new one
		if (andArray == null) {
			andArray = new ArrayNode(ESRelay.jsonNodeFactory);
		}
		filterObj.put(ESConstants.Q_AND, andArray);
	}
	
	public static ObjectNode getObjectNode(List<?> row, List<GridQueryFieldMetadata> fieldsMeta) throws Exception {
		// check if there is a query
		ObjectNode jsonQuery = new ObjectNode(ESRelay.jsonNodeFactory);
		int index=0;
		for(GridQueryFieldMetadata meta: fieldsMeta) {
			Object node = row.get(index);
			if(node==null) {
				continue;
			}
			else if(node.getClass()==String.class) {
			  jsonQuery.put(meta.fieldName(), node.toString());
			}
			else if(node.getClass()==byte[].class) {
			  jsonQuery.put(meta.fieldName(), (byte[])node);
			}
			else if(BigDecimal.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (BigDecimal)node);
			}
			else if(BigInteger.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (BigInteger)node);
			}
			else if(Long.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Long)node);
			}
			else if(Double.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Double)node);
			}
			else if(Float.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Float)node);
			}
			else if(Integer.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Integer)node);
			}
			else if(Short.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Short)node);
			}			
			else if(Byte.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Byte)node);
			}
			else if(Boolean.class==node.getClass()) {
			  jsonQuery.put(meta.fieldName(), (Boolean)node);
			}
			else if(JsonNode.class.isAssignableFrom(node.getClass())) {
			  jsonQuery.set(meta.fieldName(), (JsonNode)node);
			}
			else {
			  POJONode pnode = new POJONode(node);
			  jsonQuery.putPOJO(meta.fieldName(), pnode);
			}
			index++;
		}
		return jsonQuery;
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
	
	
}
