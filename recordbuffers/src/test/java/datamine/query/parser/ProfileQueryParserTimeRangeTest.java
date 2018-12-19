package datamine.query.parser;

import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;

public class ProfileQueryParserTimeRangeTest {

    ProfileQuery profileQuery = new ProfileQuery(
        "datamine.storage.recordbuffers.example.model");

    @Test
    public void parsingDate1() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2012/01/02, 2013/02/03]";
        profileQuery.parse(query);
        assertEquals(profileQuery.parsingResult.startDateTxt, "2012/01/02");
        assertEquals(profileQuery.parsingResult.endDateTxt, "2013/02/03");
        assertEquals(profileQuery.parsingResult.timeList.toString(), "[1325462400000, 1359935999000]");
    }

    @Test (expected = IllegalArgumentException.class)
    public void parsingDate2() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2014/01/02, 2013/02/03]";
        profileQuery.parse(query);
    }

    @Test (expected = Exception.class)
    public void parsingDate3() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2014-01-04, 2013-02-03]";
        profileQuery.parse(query);
    }

    @Test //(expected = IllegalArgumentException.class)
    public void parsingDate4() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [1325462400000, 1359935999000] tz -7";
        profileQuery.parse(query);
    }

    @Test
    public void parsingDate5() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2012/01/02, 2013/02/03] tz -7";
        profileQuery.parse(query);
        assertEquals(profileQuery.parsingResult.timeList.toString(), "[1325487600000, 1359961199000]");
    }

    @Test
    public void parsingDate6() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
        assertEquals(profileQuery.parsingResult.timeList.toString(), "[1325480400000, 1359953999000]");
    }

    @Test
    public void parsingDate7() throws ParseException {
        String query = "sel long_required_column col1 from main_table when [2012/01/02 - 31, 2013/02/03]";
        profileQuery.parse(query);
        assertEquals(profileQuery.parsingResult.startDateTxt, "2012/01/02");
        assertEquals(profileQuery.parsingResult.endDateTxt, "2013/02/03");

        SimpleDateFormat format = ParserUtil.getDateTimeFormatWithPattern("yyyy/MM/dd");
        long startTs = ParserUtil.getBeginingOfDayForDate(format.parse("2011/12/02"), DateTimeZone.UTC).getTime();
        assertEquals(profileQuery.parsingResult.timeList.toString(), "["+ startTs + ", 1359935999000]");
    }
}
