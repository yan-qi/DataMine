package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.joda.time.DateTimeZone;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Define the common behaviors of function converting the timestamp into
 * a calendar temporal information.
 *
 * The user needs to specify the time zone and formatting pattern in order
 * to have the output in right format.
 */
public abstract class GetTemporalString extends UnaryOperation {

	private String pattern;

	protected GregorianCalendar calendar;
	protected SimpleDateFormat sdf;

	protected final Column timeZoneCol;

	public GetTemporalString(Column column, Column timeZoneId, String pattern) throws Exception {
		super(column);
		this.timeZoneCol = timeZoneId;
		this.pattern = pattern;
		init();
	}

	protected void init() throws Exception {
		sdf = new SimpleDateFormat(this.pattern);
		calendar = new GregorianCalendar();
		if (getColumn().getType() != ValueUtils.LongDataType &&
			getColumn().getType() != ValueUtils.ListLongDataType) {
			throw new Exception(getColumn().getID() + " must be a long.");
		}
	}

	public void setPattern(String pattern) throws Exception {
		this.pattern = pattern;
		init();
	}

	private Value getOutput(Value tsValue, Value tzValue) {
		Number ts = (Number) tsValue.getValue();
		String tz = tzValue.getValue().toString();
		long t = ts.longValue();
		if (t < 0) {
			return new Value("unknown", ValueUtils.StringDataType);
		} else {
			TimeZone dz = DateTimeZone.forID(tz).toTimeZone();
			calendar.setTimeZone(dz);
			sdf.setTimeZone(dz);
			calendar.setTimeInMillis(t);
		}
		String s = sdf.format(calendar.getTime());
		return new Value(s, ValueUtils.StringDataType);
	}

	@Override
	public Value apply(Row input) throws Exception {
		return getOutput(col.applyOperation(input), timeZoneCol.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return getOutput(col.applyOperation(input, i), timeZoneCol.applyOperation(input, i));
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.StringDataType;
	}

}
