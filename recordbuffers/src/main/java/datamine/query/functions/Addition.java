package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * The function of adding a numeric value to the other.
 */
public class Addition extends NumericBinaryOperation {

	public Addition(Column l, Column r) throws Exception {
        super(l, r, "+");
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.plus(
			left.applyOperation(input),
			right.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return ValueUtils.plus(
			left.applyOperation(input, i),
			right.applyOperation(input, i));
	}
}