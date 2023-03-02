package de.kp.works.ignite;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */


import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.util.StdDateFormat;

import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class ValueUtils {
	private static final Logger logger = LoggerFactory.getLogger(ValueUtils.class);  
	// 属性存储类型
	public static enum PropertyType{
		STRING, 
		JSON, 
		BINARY_OBJECT, 
		ANY;
	};
	
	public static PropertyType defaultPropertyType = PropertyType.ANY;
	
	public static ObjectMapper mapper = new ObjectMapper();

    public static ValueType getValueType(Object o) {
        if (o == null) {
            return ValueType.NULL;
        } else if (o.getClass() == Boolean.class) {
            return ValueType.BOOLEAN;
        } else if (o.getClass() ==  String.class || o.getClass() ==  Character.class) {
            return ValueType.STRING;
        } else if (o.getClass() ==  Byte.class) {
            return ValueType.BYTE;
        } else if (o.getClass() ==  Short.class) {
            return ValueType.SHORT;
        } else if (o.getClass() ==  Integer.class) {
            return ValueType.INT;
        } else if (o.getClass() ==  Long.class) {
            return ValueType.LONG;
        } else if (o.getClass() ==  Float.class) {
            return ValueType.FLOAT;
        } else if (o.getClass() ==  Double.class) {
            return ValueType.DOUBLE;
        } else if (o.getClass() ==  BigDecimal.class || o.getClass() ==  BigInteger.class) {
            return ValueType.DECIMAL;
        } else if (o.getClass() ==  LocalDate.class) {
            return ValueType.DATE;
        } else if (o.getClass() ==  LocalTime.class) {
            return ValueType.TIME;
        } else if (o.getClass() ==  LocalDateTime.class) {
            return ValueType.TIMESTAMP;
        } else if (o.getClass() ==  Duration.class) {
            return ValueType.INTERVAL;
        } else if (o.getClass() ==  byte[].class) {
            return ValueType.BINARY;
        } else if (o instanceof Enum) {
            return ValueType.ENUM;
        } else if (o.getClass() ==  UUID.class) {
            return ValueType.UUID;
        } else if (o instanceof Map) {
        	if(defaultPropertyType==PropertyType.JSON) {
        		return ValueType.JSON_OBJECT;
        	}
            return ValueType.MAP;
        } else if (o instanceof Collection) {        	
            return ValueType.COLLECTION;
        } 
        else if (o.getClass().isArray()) {
        	if(defaultPropertyType==PropertyType.JSON) {
        		return ValueType.JSON_ARRAY;
        	}
            return ValueType.ARRAY;
        } 
        else if (o instanceof Serializable) {
            return ValueType.SERIALIZABLE;
        }
        else {
        	if(defaultPropertyType==PropertyType.JSON) {
        		return ValueType.JSON_OBJECT;
        	}
            throw new IllegalArgumentException("Unexpected data of type : " + o.getClass().getName());
        }
    }

    public static Object deserialize(byte[] target) {
        if (target == null) return null;
        return SerializationUtils.deserialize(target);
    }
    
    public static Object deserializeFromString(String target) {
        if (target == null) return null;
        return SerializationUtils.deserialize(Base64.getDecoder().decode(target));
    }

    public static byte[] serialize(Object o) {
        return SerializationUtils.serialize((Serializable) o);
    }

    public static String serializeToString(Object o) {
        return Base64.getEncoder().encodeToString(SerializationUtils.serialize((Serializable) o));
    }
    
    public static String serializeToJsonString(Object o) {
        try {
			return mapper.writeValueAsString(o);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return o.toString();
		}
    }
    
    public static Object parseValue(String input,ValueType toType) {
    	if(input==null) {
    		return input;
    	}
    	if(toType==ValueType.STRING) {
    		return input;
    	}
    	if(toType==ValueType.DATE) {
    		StdDateFormat fmt = new StdDateFormat();
    		try {
				return fmt.parse(input);
			} catch (ParseException e) {						
				logger.error(e.getMessage(),e);
				return input;
			}
    	}
    	if(toType==ValueType.TIMESTAMP) {
    		DateTimeFormatter fmt = DateTimeFormatter.ISO_DATE_TIME;
    		return fmt.parse(input);
    	}
    	if(toType==ValueType.INT) {
    		return Integer.valueOf(input);
    	}
    	if(toType==ValueType.SHORT) {
    		return Short.valueOf(input);
    	}
    	if(toType==ValueType.LONG) {
    		return Long.valueOf(input);
    	}
    	if(toType==ValueType.FLOAT) {
    		return Float.valueOf(input);
    	}
    	if(toType==ValueType.DOUBLE) {
    		return Double.valueOf(input);
    	}
    	if(toType==ValueType.DECIMAL) {
    		return new BigDecimal(input);
    	}
    	if(toType==ValueType.BINARY) {
    		return Base64.getDecoder().decode(input);
    	}  
    	if(toType==ValueType.ENUM) {
    		return deserializeFromString(input);
    	} 
    	if(toType==ValueType.ARRAY || toType==ValueType.MAP || toType==ValueType.SERIALIZABLE) {
    		return deserializeFromString(input);
    	}    	
    	if(toType==ValueType.JSON_ARRAY || toType==ValueType.COLLECTION) {    		
    		try {
    			return mapper.readerForListOf(String.class).readValue(input);
				
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
				return input;
			}
    	}
    	if(toType==ValueType.JSON_OBJECT) {    		
    		try {    			
				return mapper.readerForMapOf(Object.class).readValue(input);
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
				return input;
			}
    	}
    	return input;
    }
    
    // 判断id是不是document的id
    public static boolean isDocId(Object id) {
		if (id instanceof ReferenceVertexProperty) {
			return true;
		}
		return false;
	}

	public static String getDocId(Object id) {
		if (id instanceof ReferenceVertexProperty) {
			ReferenceVertexProperty<?> key = (ReferenceVertexProperty) id;
			return key.id().toString();
		}
		return id.toString();
	}	
}
