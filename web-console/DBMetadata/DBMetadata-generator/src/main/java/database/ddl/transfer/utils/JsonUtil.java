package database.ddl.transfer.utils;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.beetl.core.resource.ClasspathResource;
import org.beetl.core.resource.ClasspathResourceLoader;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName JsonUtil
 * @Description TODO
 * @Author luoyuntian
 * @Date 2019-12-25 14:32
 * @Version
 **/
public class JsonUtil {
	
	public static ObjectMapper objectMapper = new ObjectMapper();

	private volatile static Map<String, Map<String, String>> jsonMap = null;

	private JsonUtil() {

	}

	/**
	 * 读取映射数据加入到map缓存中
	 * 
	 * @throws IOException
	 */
	public static Map<String, Map<String, String>> readJsonData(String jsonPath) throws IOException {
		if (jsonMap == null) {
			synchronized (JsonUtil.class) {
				if (jsonMap == null) {
					
					ClasspathResourceLoader resourceLoader = new ClasspathResourceLoader();
					ClasspathResource resource = (ClasspathResource) resourceLoader.getResource(jsonPath);
					Reader reader = resource.openReader();
					StringBuilder jsonString = new StringBuilder();
					int ch = reader.read();
					while(ch!=-1) {
						jsonString.append((char)ch);
						ch = reader.read();
					}
					
					
					ObjectNode jsonObject = (ObjectNode) objectMapper.reader().readTree(jsonString.toString());
					jsonMap = new ConcurrentHashMap<>();
					
					Iterator<Entry<String, JsonNode>> iterator = jsonObject.fields();
					while (iterator.hasNext()) {
						Map<String, String> mapingMap = new HashMap<>();
						// 获取转换类型
						Entry<String, JsonNode> ent = iterator.next();
						String convertType = ent.getKey();
						
						// 将mapping解析为object对象
						ObjectNode mappingJson = (ObjectNode)ent.getValue();
						// 遍历mapping
						Iterator<String> orginalTypeSet = mappingJson.fieldNames();
						while (orginalTypeSet.hasNext()) {
							String orginalType = orginalTypeSet.next();
							String targetType = mappingJson.get(orginalType).asText();
							mapingMap.put(orginalType, targetType);
							jsonMap.put(convertType, mapingMap);
						}
					}
				}
			}
		}

		return jsonMap;

	}

}
