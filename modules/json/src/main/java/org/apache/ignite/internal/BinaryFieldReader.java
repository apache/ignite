package org.apache.ignite.internal;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;



public class BinaryFieldReader {
	static final byte[] ZERO_BYTES = new byte[0];
	
	/**
     * Construct from string representation to target class.
     *
     * @param cls Target class.
     * @return Object constructed from string.
     */
    @Nullable public static Object stringToObject(Class cls, String val) {
        if (val == null  || "null".equals(val) || "nil".equals(val))
            return null;

        if (String.class == cls)
            return val;

        if (Boolean.class == cls || Boolean.TYPE == cls)
            return Boolean.parseBoolean(val);

        if (Integer.class == cls || Integer.TYPE == cls)
            return Integer.parseInt(val);

        if (Long.class == cls || Long.TYPE == cls)
            return Long.parseLong(val);

        if (UUID.class == cls)
            return UUID.fromString(val);

        if (IgniteUuid.class == cls)
            return IgniteUuid.fromString(val);

        if (Byte.class == cls || Byte.TYPE == cls)
            return Byte.parseByte(val);

        if (Short.class == cls || Short.TYPE == cls)
            return Short.parseShort(val);

        if (Float.class == cls || Float.TYPE == cls)
            return Float.parseFloat(val);

        if (Double.class == cls || Double.TYPE == cls)
            return Double.parseDouble(val);

        if (BigDecimal.class == cls)
            return new BigDecimal(val);

        if (Collection.class == cls || List.class == cls)
            return Arrays.asList(val.split(";"));

        if (Set.class == cls)
            return new HashSet<>(Arrays.asList(val.split(";")));

        if (Object[].class == cls)
            return val.split(";");

        if (byte[].class == cls) {
            String[] els = val.split(";");

            if (els.length == 0 || (els.length == 1 && els[0].isEmpty()))
                return ZERO_BYTES;

            byte[] res = new byte[els.length];

            for (int i = 0; i < els.length; i ++)
                res[i] = Byte.valueOf(els[i]);

            return res;
        }

        if (cls.isEnum())
            return Enum.valueOf(cls, val);

        return val;
    }


    /** {@inheritDoc} */
    public static <F> F readNumberBinaryField(Number buf,int hdr) {
    	if(buf==null) return null;
   
        Object val = buf;

        switch (hdr) {
            case GridBinaryMarshaller.INT:
                val = buf.getClass()==Integer.class? buf: buf.intValue();
                break;

            case GridBinaryMarshaller.LONG:
            	 val = buf.getClass()==Long.class? buf: buf.longValue();

                break;               

            case GridBinaryMarshaller.SHORT:
            	val = buf.getClass()==Short.class? buf: buf.shortValue();

                break;

            case GridBinaryMarshaller.BYTE:
            	val = buf.getClass()==Byte.class? buf: buf.byteValue();

                break;               

            case GridBinaryMarshaller.FLOAT:
            	val = buf.getClass()==Float.class? buf: buf.floatValue();

                break;

            case GridBinaryMarshaller.DOUBLE:
            	val = buf.getClass()==Double.class? buf: buf.doubleValue();

                break;
            
            case GridBinaryMarshaller.DECIMAL: {
            	val = buf.getClass()==BigDecimal.class? buf: new BigDecimal(buf.toString());
                break;
            }

            case GridBinaryMarshaller.NULL:
                val = null;

                break;               
        }
        return (F)val;   
    }
    

    /** {@inheritDoc} */
    public static <F> F readOtherBinaryField(Object buf,int hdr) {
    	if(buf==null) return null;
      
        Object val = buf;;

        switch (hdr) {
            case GridBinaryMarshaller.BOOLEAN:
            	val = buf.getClass()==Boolean.class? buf: isTrue(buf);
                break;            

            case GridBinaryMarshaller.CHAR:
            	val = buf.getClass()==Character.class? buf: buf.toString().charAt(0);
                break;           

            case GridBinaryMarshaller.STRING: {
            	val = buf.toString();
                break;
            }

            case GridBinaryMarshaller.DATE: {
            	val = buf.getClass()==Date.class? buf: null;
            	if(val==null) {            		
            		DateFormat[] dateFormats = {
        				new SimpleDateFormat("yyyy-MM-dd\\'T\\'HH:mm:ss.SSS"), 
        				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), 
        				new SimpleDateFormat("yyyy-MM-dd HH:mm"),
        				new SimpleDateFormat("yyyy-MM-dd")
        			};
            		for(int i=0;i<dateFormats.length;i++) {
	            		try {
	            			val = dateFormats[i].parse(buf.toString());
	            			break;
	            		}
	            		catch(ParseException e) {
	            			
	            		}
            		}
            	}
                break;
            }

            case GridBinaryMarshaller.TIMESTAMP: {            	
            	val = buf.getClass()==Timestamp.class? buf: null;
            	if(val==null) {
            		long time = Long.parseLong(buf.toString());
            		val = new Timestamp(time);
            	}
                break;
            }

            case GridBinaryMarshaller.TIME: {
            	val = buf.getClass()==Time.class? buf: null;
            	if(val==null) {            		
            		val = Time.valueOf(buf.toString());
            	} 
                break;
            }

            case GridBinaryMarshaller.UUID: {
            	val = buf.getClass()==UUID.class? buf: UUID.fromString(buf.toString());
                break;
            }


            case GridBinaryMarshaller.NULL:
                val = null;
                break;

            default:
                // Restore buffer position.
                break;
        }

        return (F)val;   
      
    }
    
    public static boolean isTrue(Object value) {      

        if (value instanceof Boolean) {
            return ((Boolean) value).booleanValue();
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }

        if (value instanceof CharSequence) {
            return !(value.toString().isEmpty() || value.toString().isBlank());
        }
        
        if (value.getClass().getSimpleName().equalsIgnoreCase("Missing")) {
            return false;
        }

        return true;
    }
    
}
