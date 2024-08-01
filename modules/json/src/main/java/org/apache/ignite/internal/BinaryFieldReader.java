package org.apache.ignite.internal;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.ignite.internal.binary.GridBinaryMarshaller;



public class BinaryFieldReader {


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
