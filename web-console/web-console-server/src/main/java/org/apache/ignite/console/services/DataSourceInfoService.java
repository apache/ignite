package org.apache.ignite.console.services;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.binary.BinaryObject;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;


@Service
public class DataSourceInfoService {
	/** */
    private static final Logger log = LoggerFactory.getLogger(DataSourceInfoService.class);
	
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	
	
	public JsonArray fieldsInfo(JsonArray samples) {
		JsonArray columns = new JsonArray();
		if(samples.size()>0) {
			JsonObject document = samples.getJsonObject(0);
	        
        	for (String key : document.fieldNames()) {
        		JsonObject result = new JsonObject();
        		result.put("name", key);
                result.put("typeName", "VARCHAR");
                result.put("type", 12);
                result.put("unsigned", false);
                result.put("nullable", true);
	        	
	        	for(int i=0;i<samples.size();i++) {
	        		Object value = samples.getJsonObject(i).getValue(key);
		            if(value==null) {
		            	result.put("nullable", true);
		            	continue;
		            }     
		            
		            if (value instanceof BinaryObject) {
		            	try {
			            	BinaryObject bobj = (BinaryObject)value;
			            	value = bobj.deserialize();
		            	}
		            	catch(Exception e) {
		            		log.warn("BinaryObject deserialize(): {} ",e.getMessage());
		            	}
		            	
		            }
		            
		            if(value instanceof org.bson.types.Binary) {
		            	Binary bin = (Binary)(value);
		            	if(bin.length()==16) {
		            		result.put("typeName", "UUID");
		            		result.put("type", 1111);
		            	}
		            	else {
		            		result.put("typeName", "BINARY");
			            	result.put("type", -2);
		            	}
		            }		            
		            else if (value instanceof JsonObject) {
		            	result.put("typeName", "STRUCT");
		            	result.put("type", 1111);
		            } 
		            else if (value instanceof JsonArray) {
		            	result.put("typeName", "ARRAY");
		            	result.put("type", 1111);
		            } 
		            else if (value instanceof String) {
		            	result.put("typeName", "VARCHAR");
		            	result.put("type", 12);
		            }	           
		            else if (value instanceof BigInteger || value instanceof BigDecimal) {
		            	result.put("typeName", "NUMERIC");
		            	result.put("type", 2);
		            }
		            else {
		            	result.put("typeName", value.getClass().getSimpleName());
		            	result.put("type", 1111);
		            }
	            }	            
	            
	            columns.add(result);
	        }
		}        
        
        return columns;
    }
	

    public Set<String> enumFields(Map<String,Object> document) {        
        Set<String> result = new LinkedHashSet<>();
        buildFieldPaths("", document, result);
        return result;
    }

    private void buildFieldPaths(String currentPath, Object node, Set<String> result) {
        if (node instanceof Map) {
            processMap(currentPath, (Map<?, ?>) node, result);
        } else if (node instanceof List) {
            processList(currentPath, (List<?>) node, result);
        } else if (!currentPath.isEmpty()) {
            // 处理基本类型叶子节点
            result.add(currentPath);
        }
    }

    private void processMap(String basePath, Map<?, ?> map, Set<String> result) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            String newPath = basePath.isEmpty() ? key : basePath + "." + key;
            
            if (value instanceof Map || value instanceof List) {
                buildFieldPaths(newPath, value, result);
            } else {
                result.add(newPath);
            }
        }
    }

    private void processList(String basePath, List<?> list, Set<String> result) {
        for (int i = 0; i < list.size(); i++) {
            String key = Integer.toString(i);
            Object value = list.get(i);
            String newPath = basePath.isEmpty() ? key : basePath + "." + key;
            
            if (value instanceof Map || value instanceof List) {
                buildFieldPaths(newPath, value, result);
            } else {
                result.add(newPath);
            }
        }
    }
}
