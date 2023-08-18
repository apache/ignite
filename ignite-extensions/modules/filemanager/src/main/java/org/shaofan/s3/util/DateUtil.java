package org.shaofan.s3.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil {

    public static String getDateFormatToSecond(Date date) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String tag = df.format(date);
        return tag;
    }
    
    public static String getDateGMTFormat(Date date) {
    	//DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm+00:00");
        //String tag = df.format(date);
        
    	//LocalDateTime t = LocalDateTime.ofEpochSecond(date.getTime()/1000, (int)(date.getTime()%1000*1000), ZoneOffset.ofHours(8));
    	//ZonedDateTime zt = ZonedDateTime.of(t, ZoneId.systemDefault());
    	//String tag = zt.format(DateTimeFormatter.ISO_INSTANT);       
        return date.toGMTString();
    }

    public static String getDateTagToSecond() {
        DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String tag = df.format(new Date());
        return tag;
    }

    public static String getUTCDateFormat() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        String tag = df.format(new Date());
        return tag;
    }    
    
}
