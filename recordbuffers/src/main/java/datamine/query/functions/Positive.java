package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class Positive extends NumericUnaryOperation {

	public Positive(Column l) throws Exception {
        super(l, "+");
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.multiply(
			col.applyOperation(input),
			new Value(1, ValueUtils.LongDataType));
	}
}