package datamine.query.functions;

import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * Define the behavior of counting the events. It doesn't require any
 * input for particular columns.
 *
 * Note that only count(*) is supported.
 */
public class Count extends AggOperation {

	private Value one;

	public Count() throws Exception {
		super(null);
		one = new Value((long) 1, ValueUtils.LongDataType);
	}

	@Override
	public Value apply(Row input) throws Exception {
		return one;
	}

	@Override
	public Value apply(Value base, Value delta) throws Exception {
		if (base == null) {
			return delta;
		}
		return ValueUtils.plus(base, delta);
	}

	@Override
	public Value apply(String key, Value base, Value delta) throws Exception {
		return apply(base, delta);
	}

	@Override
	public String getID() {
		return "count(*)";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.LongDataType;
	}

}
