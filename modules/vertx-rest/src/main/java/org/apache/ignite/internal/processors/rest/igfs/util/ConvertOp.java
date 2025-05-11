package org.apache.ignite.internal.processors.rest.igfs.util;

import java.math.BigDecimal;

import org.springframework.util.StringUtils;

public class ConvertOp {

    public static boolean isNull(Object obj) {
        if (obj == null) {
            return true;
        } else {
            return false;
        }
    }

    public static String convert2String(Object obj) {        
        if (!isNull(obj)) {
        	return obj.toString();
        } else {
            return "";
        }        
    }

    public static Integer convert2Int(Object obj) {
        Integer result;
        if (!isNull(obj)) {
            if (obj.getClass() == BigDecimal.class) {
                BigDecimal bigDecimal = (BigDecimal) obj;
                return bigDecimal.intValue();
            } 
            else if (obj instanceof Number) {
            	Number bigDecimal = (Number) obj;
                return bigDecimal.intValue();
            }
            else {
                try {
                    result = Integer.parseInt(String.valueOf(obj));
                } catch (Exception e) {
                    e.printStackTrace();
                    ;
                    result = 0;
                }
            }

        } else {
            result = 0;
        }
        return result;
    }
}
