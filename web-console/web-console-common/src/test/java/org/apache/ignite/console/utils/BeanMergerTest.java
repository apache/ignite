package org.apache.ignite.console.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;


import lombok.Data;

@Data
class Bean1 {
    private int id;
    private String name;
    private Integer[] array;
    private List<String> list;
    private Map<String, String> map;

    // 构造函数、getter和setter省略
}


public class BeanMergerTest {
	
    /** */
    @Test
    public void testDummy() {
      
        Bean1 bean1 = new Bean1();
        bean1.setId(1);
        bean1.setName("Bean1");
        bean1.setArray(new Integer[]{1, 2, 3});
        bean1.setList(new ArrayList<>(List.of("A", "B")));
        bean1.setMap(new HashMap<>() {{ put("key1", "value1"); put("key2", "value2"); }});

        Bean1 bean2 = new Bean1();
        bean2.setId(2);
        bean2.setName("Bean2");
        bean2.setArray(new Integer[]{4, 5, 6});
        bean2.setList(new ArrayList<>(List.of("C", "D")));
        bean2.setMap(new HashMap<>() {{ put("key3", "value3"); put("key4", "value4"); }});

        BeanMerger.mergeBeans(bean1, bean2);
        
        String[] s = {"qww","vv0"};
        if(s instanceof Object[]) {
        	Object[] os = s;
        }
        
        Integer[] ia = {4, 5, 6};
        if(ia instanceof Object[]) {
        	Object[] os = s;
        }

        System.out.println("Merged Bean2: " + bean2);
    }
}
