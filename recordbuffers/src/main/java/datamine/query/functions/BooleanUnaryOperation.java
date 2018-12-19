package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.ValueUtils;

/**
 * Define an abstract class for the unary boolean operations.
 */
public abstract class BooleanUnaryOperation extends UnaryOperation {

	private final String operator;

	public BooleanUnaryOperation(Column l, String operator) throws Exception {
        super(l);
		if (ValueUtils.BooleanDataType != l.getType()) {
			throw new IllegalArgumentException(
				"Only boolean or binary values are allowed for " + operator);
		}
		this.operator = operator;
	}


	@Override
	public String getID() {
		return "(" + operator + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}
}