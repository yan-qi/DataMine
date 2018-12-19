package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;

import java.util.Map;

/**
 * Define the behavior of the constant operation. Basically it can be
 * treated as a wrapper of any immutable value.
 */
public class Constant extends NullaryOperation {

	private Value constant;

	public Constant(Value v) {
		constant = v;
	}

	@Override
	public Value apply(Row input) {
		return constant;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return constant;
	}

	@Override
	public String getID() {
		return constant.toString();
	}

	@Override
	public boolean applicable(Map<String, Column> output) {
		return true;
	}

	@Override
	public int getResultValueType() {
		return constant.getType();
	}
}
