package datamine.query.parser.data;

import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.interfaces.ProfileInterface;
import datamine.query.example.wrapper.builder.RecordBuffersBuilder;
import datamine.query.parser.ParserUtil;
import datamine.storage.api.RecordBuilderInterface;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.joda.time.DateTimeZone.UTC;

public class ExampleProfileDataGenerator {

    String dateStr1 = "2017/11/01";
    String dateStr2 = "2017/11/05";

    private long getTimestamp(String dateStr) {
        SimpleDateFormat sdf = ParserUtil.getDateTimeFormatWithPattern("yyyy/MM/dd");
        Date date = null;
        try {
            date = ParserUtil.getBeginingOfDayForDate(
                sdf.parse(dateStr),
                UTC);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public Object createProfileRecord1() {

        RecordBuilderInterface builder = new RecordBuffersBuilder();
        ProfileInterface profile = builder.build(ProfileInterface.class);
        profile.setProfileId("1");
        long ts = getTimestamp(dateStr1);
        profile.setCreationTime(ts);
        profile.setLastModifiedTime(ts + 100023);

        List<EventInterface> eventInterfaceList = new ArrayList<>();

        EventInterface event1 = builder.build(EventInterface.class);
        event1.setEventId("1-1");
        event1.setEventTime(ts);
        event1.setStart(10d);
        event1.setDuration(12.2d);

        event1.setUrl("http://abc");

        BasicInterface basicAttr = builder.build(BasicInterface.class);
        basicAttr.setEventTime(ts);
        event1.setBasicAttrs(basicAttr);
        eventInterfaceList.add(event1);

        EventInterface event2 = builder.build(EventInterface.class);
        event2.setEventId("1-2");
        event2.setEventTime(ts + 10000);
        event2.setStart(0d);
        event2.setDuration(11.2d);
        event2.setUrl("http://bcd");
        basicAttr = builder.build(BasicInterface.class);
        basicAttr.setEventTime(ts + 10000);
        event2.setBasicAttrs(basicAttr);
        eventInterfaceList.add(event2);

        event2 = builder.build(EventInterface.class);
        event2.setEventId("1-3");
        event2.setEventTime(ts + 100023);
        event2.setStart(10d);
        event2.setDuration(123.3d);
        event2.setUrl("http://bcd");
        basicAttr = builder.build(BasicInterface.class);
        basicAttr.setEventTime(ts + 100023);
        event2.setBasicAttrs(basicAttr);
        eventInterfaceList.add(event2);

        profile.setEvents(eventInterfaceList);

        return profile.getBaseObject();
    }


    public Object createProfileRecord2() {

        RecordBuilderInterface builder = new RecordBuffersBuilder();
        ProfileInterface profile = builder.build(ProfileInterface.class);
        profile.setProfileId("2");
        long ts = getTimestamp(dateStr2);
        profile.setCreationTime(ts);
        profile.setLastModifiedTime(ts + 100089);

        List<EventInterface> eventInterfaceList = new ArrayList<>();

        EventInterface event1 = builder.build(EventInterface.class);
        event1.setEventId("2-1");
        event1.setEventTime(ts);
        event1.setStart(200d);
        event1.setDuration(212.2d);
        event1.setUrl("http://abc2");
        eventInterfaceList.add(event1);

        EventInterface event2 = builder.build(EventInterface.class);
//        event2.setEventId("2-2");
        event2.setEventTime(ts + 10000);
        event2.setStart(20d);
        event2.setDuration(123.2d);
        event2.setUrl("http://bcd2");
        eventInterfaceList.add(event2);

        event2 = builder.build(EventInterface.class);
//        event2.setEventId("2-3");
        event2.setEventTime(ts + 100089);
        event2.setStart(11d);
        event2.setDuration(1.3d);
//        event2.setUrl("http://bcd3");
        eventInterfaceList.add(event2);

        profile.setEvents(eventInterfaceList);

        return profile.getBaseObject();
    }

}
