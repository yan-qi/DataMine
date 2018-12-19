package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class Sum extends AggOperation {

	public Sum(Column c) throws Exception {
        super(c);
		if (!ValueUtils.isNumbericType(c.getType())) {
			throw new Exception(c.getID() + " must be a number.");
		}
	}

	// Init
	@Override
	public Value apply(Row input) throws Exception {
		return col.applyOperation(input);
	}

	// Iter
	@Override
	public Value apply(Value prev, Value curr) throws Exception {
		if (prev == null) {
			return curr;
		}
		return ValueUtils.plus(prev, curr);

	}

	// Interface for aggregates over nested table
	@Override
	public Value apply(String key, Value prev, Value curr) throws Exception {
		return apply(prev, curr);
	}

	@Override
	public String getID() {
		return "sum(" + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		if (col.getType() == ValueUtils.LongDataType) {
			return ValueUtils.LongDataType;
		} else {
			return ValueUtils.FloatDataType;
		}
	}
}