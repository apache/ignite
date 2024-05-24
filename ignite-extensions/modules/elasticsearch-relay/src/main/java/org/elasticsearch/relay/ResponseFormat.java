package org.elasticsearch.relay;

import java.util.NoSuchElementException;

/**
 * JSON 返回的数据布局格式
 * @author zjf
 *
 */
public enum ResponseFormat {
	 FIELDSMETA("query"), // CacheQueryResult { items =List<List<?>>, fieldsMeta=List<> } 
	 DATASET("dataset"),    // List<Map<String,Object>>   { rootPath: [{},{}] }
	 HITS("hits"),       // Elasticsearch hits format
	 OPERATION("operation");  // Elasticsearch op result, { rootPath: {} }
	 
	 private String code;
	 
	 private ResponseFormat(String code){
		 this.code=code;
	 }
	 
	 public static ResponseFormat of(String code) {
		 for(ResponseFormat e: values()) {
			 if(e.code.equalsIgnoreCase(code)) {
				 return e;
			 }
			 if(e.name().equalsIgnoreCase(code)) {
				 return e;
			 }
		 }
		 throw new NoSuchElementException(code);
	 }
}
