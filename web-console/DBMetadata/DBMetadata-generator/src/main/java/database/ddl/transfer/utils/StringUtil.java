package database.ddl.transfer.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 字符串工具类
 *
 * @author gs
 */
public final class StringUtil {

	/**
	 * 输出Map中的信息
	 * 
	 * @param stringMap Map
	 * @return Map中的信息
	 */
	public static String toString(Map<String, String> stringMap) {
		StringBuilder stringBuilder = new StringBuilder("[");
		if (stringMap != null && !stringMap.isEmpty()) {
			Iterator<Map.Entry<String, String>> iterator = stringMap.entrySet().iterator();
			Map.Entry<String, String> entry = null;
			while (iterator.hasNext()) {
				entry = iterator.next();

				stringBuilder.append(entry.getKey()).append(":");
				if (entry.getValue() != null) {
					stringBuilder.append(entry.getValue());
				} else {
					stringBuilder.append("null");
				}
				stringBuilder.append(",");
			}

			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
		}
		stringBuilder.append("]");

		return stringBuilder.toString();
	}

	/**
	 * 判断字符串是否为空
	 * 
	 * @param string 字符串
	 * @return 若为null或全部为空白，则返回true
	 */
	public static boolean isBlank(String string) {
		return string == null || string.trim().length() == 0;
	}


	/**
	   * @author luoyuntian
	   * @date 2019-12-31 10:29
	   * @description 将字符串按逗号拆分成map
	    * @param string
	   * @return
	   */
	public static Map<String,String> str2Map(String string){
		Map<String,String> strMap = new HashMap<>();
		String[] result =  string.split(",");
		for(String str:result){
			strMap.put(str.toUpperCase(),null);
		}
		return strMap;
	}

	private StringUtil() {
	}
}
