package datamine.query.parser;

import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ParserUtil {

    private ParserUtil() {}

    public static SimpleDateFormat getDateTimeFormatWithPattern(String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);
        return formatter;
    }

    public static Date getEndOfDayForDate(Date dateTime, DateTimeZone timeZone) {
        MutableDateTime mDate = new MutableDateTime(dateTime.getTime(), timeZone);
        mDate.setHourOfDay(23);
        mDate.setMinuteOfHour(59);
        mDate.setSecondOfMinute(59);
        mDate.setMillisOfSecond(0);
        return mDate.toDate();
    }

    public static Date getBeginingOfDayForDate(Date dateTime, DateTimeZone timeZone) {
        MutableDateTime mDate = new MutableDateTime(dateTime.getTime(), timeZone);
        mDate.setHourOfDay(0);
        mDate.setMinuteOfHour(0);
        mDate.setSecondOfMinute(0);
        mDate.setMillisOfSecond(0);
        return mDate.toDate();
    }

    public static Date getPreviousDate(DateTimeZone timeZone) {
        MutableDateTime mDate = new MutableDateTime(timeZone);
        mDate.addDays(-1);
        mDate.setHourOfDay(0);
        mDate.setMinuteOfHour(0);
        mDate.setSecondOfMinute(0);
        mDate.setMillisOfSecond(0);
        return mDate.toDate();

    }

    public static Date getCurrentDate(DateTimeZone timeZone) {
        MutableDateTime mDate = new MutableDateTime(timeZone);
        mDate.setHourOfDay(0);
        mDate.setMinuteOfHour(0);
        mDate.setSecondOfMinute(0);
        mDate.setMillisOfSecond(0);
        return mDate.toDate();

    }

    public static Date getDateBeforeNumDays(Date fromDate, int numDays, DateTimeZone timeZone) {
        MutableDateTime mDate = new MutableDateTime(fromDate.getTime(), timeZone);
        mDate.addDays(-numDays);
        mDate.setHourOfDay(0);
        mDate.setMinuteOfHour(0);
        mDate.setSecondOfMinute(0);
        mDate.setMillisOfSecond(0);
        return mDate.toDate();
    }
}
