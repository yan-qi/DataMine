package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.ValueUtils;

/**
 * Describe the attributes and behaviors of binary function with numeric input and output.
 *
 * The data type includes Long and Float.
 */
public abstract class NumericBinaryOperation extends BinaryOperation {

    /**
     * A string to identify the operator, such as "+", "-", "*", "/", "%" etc.
     */
	private final String operator;

	public NumericBinaryOperation(Column l, Column r, String operator) throws Exception {
        super(l, r);
		if (!ValueUtils.isNumbericType(l.getType()) ||
			!ValueUtils.isNumbericType(r.getType())) {
			throw new IllegalArgumentException("Only numbers are allowed for " + operator);
		}

		this.operator = operator;
	}


	@Override
	public String getID() {
		return "(" + left.getID() + operator + right.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		if (left.getType() == ValueUtils.LongDataType &&
			right.getType() == ValueUtils.LongDataType) {
			return ValueUtils.LongDataType;
		} else {
			return ValueUtils.FloatDataType;
		}
	}
}