package org.shaofan.s3.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import software.amazon.awssdk.utils.DateUtils;

public class DateUtil {
	

    public static String getDateFormatToSecond(Date date) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String tag = df.format(date);
        return tag;
    }
    
    public static String getDateGMTFormat(Date date) {
    	DateFormat df = new SimpleDateFormat("ddd, DD MMM YYYY HH:mm:ss ZZ");
        String tag = df.format(date);
        
        String str = DateUtils.formatIso8601Date(Instant.ofEpochMilli(date.getTime()));
        return str;
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
