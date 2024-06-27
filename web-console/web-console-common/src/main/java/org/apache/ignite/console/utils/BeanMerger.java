package org.apache.ignite.console.utils;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

import org.apache.commons.lang3.ArrayUtils;



public class BeanMerger {
	
	/**
	 * 将Bean1的数据合并到Bean2中， 遇到数组或者List，进行数组内元素合并，遇到Map进行Map项目合并。空值跳过
	 * @param source
	 * @param target
	 */
    public static void mergeBeans(Object source, Object target) {
        Class<?> sourceClass = source.getClass();
        Class<?> targetClass = target.getClass();

        for (Field field : sourceClass.getDeclaredFields()) {
            try {
            	if(Modifier.isStatic(field.getModifiers())) {
            		continue;
            	}
                Field targetField = targetClass.getDeclaredField(field.getName());

                // 设置访问权限以访问私有字段
                field.setAccessible(true);
                targetField.setAccessible(true);

                Object sourceValue = field.get(source);
                if(sourceValue == null) {
                	continue;
                }
                
                Object targetValue = targetField.get(target);

                if (sourceValue instanceof Object[] && targetValue instanceof Object[]) {
                	Object[] mergedArray = mergeArrays((Object[]) sourceValue, (Object[]) targetValue);
                    targetField.set(target, mergedArray);
                } else if (sourceValue instanceof List && targetValue instanceof List) {
                    List<?> mergedList = mergeLists((List) sourceValue, (List<?>) targetValue);
                    targetField.set(target, mergedList);
                } else if (sourceValue instanceof Map && targetValue instanceof Map) {
                    Map<?, ?> mergedMap = mergeMaps((Map) sourceValue, (Map<?, ?>) targetValue);
                    targetField.set(target, mergedMap);
                } else {
                    targetField.set(target, sourceValue);
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    private static <T> T[] mergeArrays(T[] source, T[] target) {
    	T[] merged = ArrayUtils.addAll(target, source);    	       
        return merged;
    }

    private static <T> List<T> mergeLists(List<T> source, List<T> target) {
        List<T> merged = new ArrayList<>(source);
        merged.addAll(target);
        return merged;
    }

    private static <K, V> Map<K, V> mergeMaps(Map<K, V> source, Map<K, V> target) {
        Map<K, V> merged = new HashMap<>(source);
        merged.putAll(target);
        return merged;
    }
}
