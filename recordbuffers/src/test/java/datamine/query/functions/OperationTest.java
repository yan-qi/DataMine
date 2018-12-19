package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.junit.Test;
import org.testng.Assert;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static datamine.query.data.ValueUtils.nullValue;

public class OperationTest {

    Row row = Row.newInstance();

    public OperationTest() throws ParseException {
        row.setColumnValue("longLeft", new Value(100l));
        row.setColumnValue("longRight", new Value(202l));

        row.setColumnValue("floatLeft", new Value(10.01));
        row.setColumnValue("floatRight", new Value(2.12));

        row.setColumnValue("playStart", new Value(100.0));
        row.setColumnValue("playDuration", new Value(10.0));
        row.setColumnValue("binSize", new Value(5.0));

        row.setColumnValue("jsonTxt", new Value("{\"page\": \"welcome\"}"));
        row.setColumnValue("jsonTxt1", new Value("page=welcome"));
        row.setColumnValue("jsonQuery", new Value("page"));

        row.setColumnValue("startPos", new Value(0l));
        row.setColumnValue("endPos", new Value(4l));

        DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse("01/04/2017");
        long time = date.getTime();

        row.setColumnValue("eventTime", new Value(time));
    }

    @Test
    public void testAddition() throws Exception {
        Addition add =
            new Addition(
                Column.newInstance("longLeft", ValueUtils.LongDataType),
                Column.newInstance("longRight", ValueUtils.LongDataType));

        Assert.assertEquals(add.apply(row).getLongValue(), 302l);

        add =
            new Addition(
                Column.newInstance("floatLeft", ValueUtils.FloatDataType),
                Column.newInstance("floatRight", ValueUtils.FloatDataType));

        Assert.assertEquals(add.apply(row).getDoubleValue(), 12.13, 0.001);
    }

    @Test
    public void testSubtraction() throws Exception {
        Subtraction subtraction =
            new Subtraction(
                Column.newInstance("longLeft", ValueUtils.LongDataType),
                Column.newInstance("longRight", ValueUtils.LongDataType));

        Assert.assertEquals(subtraction.apply(row).getLongValue(), -102);

        subtraction =
            new Subtraction(
                Column.newInstance("floatLeft", ValueUtils.FloatDataType),
                Column.newInstance("floatRight", ValueUtils.FloatDataType));

        Assert.assertEquals(subtraction.apply(row).getDoubleValue(), 7.89, 0.001);
    }

    @Test
    public void testMultiplication() throws Exception {
        Multiplication multiplication =
            new Multiplication(
                Column.newInstance("longLeft", ValueUtils.LongDataType),
                Column.newInstance("longRight", ValueUtils.LongDataType));

        Assert.assertEquals(multiplication.apply(row).getLongValue(), 20200);

        multiplication =
            new Multiplication(
                Column.newInstance("floatLeft", ValueUtils.FloatDataType),
                Column.newInstance("floatRight", ValueUtils.FloatDataType));

        Assert.assertEquals(multiplication.apply(row).getDoubleValue(), 21.221, 0.001);
    }

    @Test
    public void testDivision() throws Exception {
        Division d =
            new Division(
                Column.newInstance("longLeft", ValueUtils.LongDataType),
                Column.newInstance("longRight", ValueUtils.LongDataType));

        Assert.assertEquals(d.apply(row).getLongValue(), 0);

        row.setColumnValue("longRight", new Value(0));

        Assert.assertEquals(d.apply(row), nullValue);

        d =
            new Division(
                Column.newInstance("floatLeft", ValueUtils.FloatDataType),
                Column.newInstance("floatRight", ValueUtils.FloatDataType));

        Assert.assertEquals(d.apply(row).getDoubleValue(), 4.722, 0.001);

        row.setColumnValue("floatRight", new Value(0));

        Assert.assertEquals(d.apply(row).getValue(), Float.NaN);
    }

    @Test
    public void testModulo() throws Exception {
        Modulo d =
            new Modulo(
                Column.newInstance("longRight", ValueUtils.LongDataType),
                Column.newInstance("longLeft", ValueUtils.LongDataType));

        Assert.assertEquals(d.apply(row).getLongValue(), 2);

        row.setColumnValue("longLeft", new Value(0));

        Assert.assertEquals(d.apply(row), nullValue);

        d =
            new Modulo(
                Column.newInstance("floatLeft", ValueUtils.FloatDataType),
                Column.newInstance("floatRight", ValueUtils.FloatDataType));

        Assert.assertEquals(d.apply(row).getDoubleValue(), 1.53, 0.001);

        row.setColumnValue("floatRight", new Value(0));

        Assert.assertEquals(d.apply(row).getValue(), Float.NaN);
    }

    @Test
    public void testBin() throws Exception {
        Bin bin =
            new Bin(
                Column.newInstance("playStart", ValueUtils.FloatDataType),
                Column.newInstance("playDuration", ValueUtils.FloatDataType),
                Column.newInstance("binSize", ValueUtils.FloatDataType));

        List<Long> ends = (List<Long>) bin.apply(row).getValue();
        Assert.assertEquals(ends.size(), 2);
        Assert.assertEquals(ends.get(0).longValue(), 20l);
        Assert.assertEquals(ends.get(1).longValue(), 22);
    }

    @Test
    public void testCastLong() throws Exception {
        CastLong cl = new CastLong(Column.newInstance("floatRight", ValueUtils.FloatDataType));
        Assert.assertEquals(cl.apply(row).getLongValue(), 2l);
    }

    @Test
    public void testCoalesce() throws Exception {
        List<Column> cols = new ArrayList<>();
        cols.add(Column.newInstance("floatRight1", ValueUtils.FloatDataType));
        Coalesce coal = new Coalesce(cols);
        Assert.assertEquals(coal.apply(row), ValueUtils.nullValue);

        cols.add(Column.newInstance("floatRight", ValueUtils.FloatDataType));
        coal = new Coalesce(cols);
        Assert.assertEquals(coal.apply(row).getLongValue(), 2l);
    }

    @Test
    public void testConcatenate() throws Exception {
        List<Column> cols = new ArrayList<>();
        cols.add(Column.newInstance("floatRight1", ValueUtils.FloatDataType));
        Concat coal = new Concat(cols);
        Assert.assertEquals(coal.apply(row).getValue().toString(), ValueUtils.nullValue.toString());

        cols.add(Column.newInstance("floatRight", ValueUtils.FloatDataType));
        coal = new Concat(cols);
        Assert.assertEquals(coal.apply(row).toString(), "null2.12");
    }

    @Test
    public void testConstant() throws Exception {
        Constant constant = new Constant(new Value(123l));
        Assert.assertEquals(constant.apply(row), constant.apply(row, 0));
    }

    @Test
    public void testTemporalConversion() throws Exception {
        Constant tz = new Constant(new Value("UTC", ValueUtils.StringDataType));
        Column tzCol = Column.newInstance("StringConst-" + tz.getID(), tz);
        GetDate getDate = new GetDate(
            Column.newInstance("eventTime", ValueUtils.LongDataType), tzCol
        );
        Assert.assertEquals(getDate.apply(row).toString(), "2017/04/01");

        GetDayInWeek getDayInWeek = new GetDayInWeek(
            Column.newInstance("eventTime", ValueUtils.LongDataType), tzCol
        );
        Assert.assertEquals(getDayInWeek.apply(row).toString(), "Sat");

        GetHour getHour = new GetHour(
            Column.newInstance("eventTime", ValueUtils.LongDataType), tzCol
        );
        Assert.assertEquals(getHour.apply(row).toString(), "00");

        GetWeekInYear getWeekInYear = new GetWeekInYear(
            Column.newInstance("eventTime", ValueUtils.LongDataType), tzCol
        );
        Assert.assertEquals(getWeekInYear.apply(row).toString(), "13");

        GetMonth getMonth = new GetMonth(
            Column.newInstance("eventTime", ValueUtils.LongDataType), tzCol
        );
        Assert.assertEquals(getMonth.apply(row).toString(), "2017/04");

        GetTimestamp getTimestamp = new GetTimestamp(
            Column.newInstance("ts", new Constant(new Value("2017/04/01")))
        );
        getTimestamp.setTimeZoneId("UTC");
        Assert.assertEquals(getTimestamp.apply(row).toString(),
            row.getColumnValue("eventTime").toString());
    }

    @Test
    public void testCount() throws Exception {
        Count count = new Count();
        Assert.assertEquals(count.apply(row).getLongValue(), 1);
        Assert.assertEquals(count.apply(new Value(1l), new Value(2l)).getLongValue(), 3);
        Assert.assertEquals(count.apply(null, new Value(2l)).getLongValue(), 2);
        Assert.assertEquals(
            count.apply("", new Value(1l), new Value(2l)).getLongValue(), 3);
    }

    @Test
    public void testCountDistinct() throws Exception {

    }

    @Test
    public void testEqual() throws Exception {
        Equal eq = new Equal(
            Column.newInstance("longLeft", ValueUtils.LongDataType),
            Column.newInstance("longRight", ValueUtils.LongDataType)
        );
        Assert.assertFalse((Boolean) eq.apply(row).getValue());
    }

    @Test
    public void jsonVal() throws Exception {
        GetJsonValue gj = new GetJsonValue(
                Column.newInstance("jsonTxt", ValueUtils.StringDataType),
                Column.newInstance("jsonQuery", ValueUtils.StringDataType)
        );
        Assert.assertEquals(gj.apply(row).getValue().toString(), "welcome");

        gj = new GetJsonValue(
                Column.newInstance("jsonTxt1", ValueUtils.StringDataType),
                Column.newInstance("jsonQuery", ValueUtils.StringDataType)
        );
        Assert.assertEquals(gj.apply(row), ValueUtils.nullValue);
    }

    @Test
    public void substr() throws Exception {
        Substr ss = new Substr(
                Column.newInstance("jsonTxt1", ValueUtils.StringDataType),
                Column.newInstance("startPos", ValueUtils.LongDataType),
                Column.newInstance("endPos", ValueUtils.LongDataType)
        );
        Assert.assertEquals(ss.apply(row).getValue().toString(), "page");
    }

    @Test
    public void extStr() throws Exception {
        ExtStr es = new ExtStr(
                Column.newInstance("jsonTxt1", ValueUtils.StringDataType),
                "[a-z]+"
        );
        Assert.assertEquals(es.apply(row).getValue().toString(), "page");
    }

    public void testMinMax() throws Exception {
    }

    public void testCashWhen() throws Exception {
        CaseStatement case1 = new CaseStatement(new Column[] {}, new Column[] {});
    }
}
