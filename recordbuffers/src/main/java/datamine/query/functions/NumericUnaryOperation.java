package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public abstract class NumericUnaryOperation extends UnaryOperation {

	private final String operator;

	public NumericUnaryOperation(Column l, String operator) throws Exception {
        super(l);
		if (!ValueUtils.isNumbericType(l.getType())) {
			throw new IllegalArgumentException("Only numbers are allowed for " + operator);
		}
		this.operator = operator;
	}


	@Override
	public String getID() {
		return "(" + operator + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		if (col.getType() == ValueUtils.LongDataType) {
			return ValueUtils.LongDataType;
		} else {
			return ValueUtils.FloatDataType;
		}
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		throw new Exception("Numeric operation does not support list type operation.");
	}
}