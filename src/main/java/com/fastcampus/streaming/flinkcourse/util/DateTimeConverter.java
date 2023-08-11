package com.fastcampus.streaming.flinkcourse.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeConverter {
    public static ZonedDateTime epochMilliToZonedDateTime(long epochMilli, ZoneId zoneId) {
        Instant instant = Instant.ofEpochMilli(epochMilli);
        return ZonedDateTime.ofInstant(instant, zoneId);
    }
}
