package org.elasticsearch.relay.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.model.ESQuery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
}
