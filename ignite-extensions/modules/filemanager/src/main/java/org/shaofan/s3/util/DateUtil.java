package org.shaofan.s3.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil {

    public static String getDateFormatToSecond(Date date) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String tag = df.format(date);
        return tag;
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
