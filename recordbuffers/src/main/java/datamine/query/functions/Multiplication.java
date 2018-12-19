package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * The function of multiplying a numeric value to the other.
 */
public class Multiplication extends NumericBinaryOperation {

	public Multiplication(Column l, Column r) throws Exception {
        super(l, r, "x");
	}

	@Override
	public Value apply(Row input) throws Exception {
		Value left = this.left.applyOperation(input);
		Value right = this.right.applyOperation(input);
		return ValueUtils.multiply(left, right);
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		Value left = this.left.applyOperation(input, i);
		Value right = this.right.applyOperation(input, i);
		return ValueUtils.multiply(left, right);
	}
}