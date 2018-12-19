package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.joda.time.DateTimeZone;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Get the timestamp of a given calendar date.
 *
 * The user needs to specify the time zone and formatting pattern in order
 * to have the output in right format.
 */
public class GetTimestamp extends UnaryOperation {

	private String pattern;
	private String timeZoneId;

	protected GregorianCalendar calendar;
	protected SimpleDateFormat sdf;

	public GetTimestamp(Column column) throws Exception {
		super(column);
		setPattern("yyyy/MM/dd");
		setTimeZoneId("UTC");
	}

	public GetTimestamp(Column column, String pattern, String timeZoneId) throws Exception {
		super(column);
		setPattern(pattern);
		setTimeZoneId(timeZoneId);
	}

	public void setPattern(String pattern) throws Exception {
		this.pattern = pattern;
		init();
	}

	public void setTimeZoneId(String timeZoneId) throws Exception {
		this.timeZoneId = timeZoneId;
		init();
	}

	protected void init() throws Exception {
		sdf = new SimpleDateFormat(this.pattern);
		TimeZone tz = DateTimeZone.forID(this.timeZoneId).toTimeZone();
		sdf.setTimeZone(tz);
		calendar = new GregorianCalendar(tz);
		if (getColumn().getType() != ValueUtils.StringDataType &&
			getColumn().getType() != ValueUtils.ListStringDataType) {
			throw new Exception(getColumn().getID() + " must be a string.");
		}
	}

	private Value getOutput(Value input) throws ParseException {
		String dateStr = input.toString();
		Date date = sdf.parse(dateStr);
		return new Value(date.getTime(), ValueUtils.LongDataType);
	}

	@Override
	public Value apply(Row input) throws Exception {
		return getOutput(col.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return getOutput(col.applyOperation(input, i));
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.LongDataType;
	}

	@Override
	public String getID() {
		return "toTimestamp(" + col.getID() + " with pattern " + pattern
			+ " and timezone at " + timeZoneId + ")";
	}
}
