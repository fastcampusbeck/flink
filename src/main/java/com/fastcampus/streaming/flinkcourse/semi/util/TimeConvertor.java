package com.fastcampus.streaming.flinkcourse.semi.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeConvertor {
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static long convertStringToTimestamp(String dateTimeString) throws ParseException {
        Date date = FORMATTER.parse(dateTimeString);
        return date.getTime();
    }

    public static String convertTimestampToString(long timestamp) {
        Date date = new Date(timestamp);
        return FORMATTER.format(date);
    }
}
