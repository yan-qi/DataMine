package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;


/**
 * The function of subtracting a numeric value from the other.
 */
public class Subtraction extends NumericBinaryOperation {

	public Subtraction(Column l, Column r) throws Exception {
        super(l, r, "-");
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.minus(
			left.applyOperation(input),
			right.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return ValueUtils.minus(
			left.applyOperation(input, i),
			right.applyOperation(input, i));
	}
}