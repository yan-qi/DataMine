package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.ValueUtils;

/**
 * Define an abstract class for the boolean binary operations.
 */
public abstract class BooleanBinaryOperation extends BinaryOperation {

	private final String operator;

	public BooleanBinaryOperation(Column l, Column r, String operator) throws Exception {
        super(l, r);
		if (l.getType() != ValueUtils.BooleanDataType || r.getType() != ValueUtils.BooleanDataType) {
			throw new IllegalArgumentException("Only boolean values are allowed for " + operator);
		}
		this.operator = operator;
	}


	@Override
	public String getID() {
		return "(" + left.getID() + " " + operator + " " + right.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}

}