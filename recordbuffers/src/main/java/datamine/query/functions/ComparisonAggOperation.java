package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * Define the behaviors of aggregation that gets the result through comparison.
 */
public abstract class ComparisonAggOperation extends AggOperation {

	boolean isString = false;

	public ComparisonAggOperation(Column c) throws Exception {
		super(c);
		if (!ValueUtils.isNumbericType(c.getType()) &&
			c.getType() != ValueUtils.StringDataType) {
			throw new Exception(c.toString() + " is not a number or a string.");
		}

		if (c.getType() == ValueUtils.StringDataType) {
			isString = true;
		}
	}

    @Override
    public Value apply(Row input) throws Exception {
        return col.applyOperation(input);
    }

    @Override
    public Value apply(String key, Value base, Value delta) throws Exception {
        return apply(base, delta);
    }

	@Override
	public int getResultValueType() {
		if (col.getType() == ValueUtils.StringDataType) {
			return ValueUtils.StringDataType;
		} else if (col.getType() == ValueUtils.LongDataType) {
			return ValueUtils.LongDataType;
		} else {
			return ValueUtils.FloatDataType;
		}
	}

}