package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Operation;

import java.util.Map;

/**
 * Define the behavior of unary function.
 */
public abstract class UnaryOperation implements Operation {

	final Column col;

	public UnaryOperation(Column input) {
		col = input;
	}

	@Override
	public boolean applicable(Map<String, Column> columnMap) {
		return col.applicable(columnMap);
	}
	
	public Column getColumn() {
		return col;
	}

}
