package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Operation;

import java.util.Map;

/**
 * Define the attributes and behaviors of the binary function.
 *
 * A binary function takes two operands as input.
 */
public abstract class BinaryOperation implements Operation {

	/**
	 * The Operands of the function.
	 */
	protected Column left, right;


	public BinaryOperation(Column l, Column r) {
		left = l;
		right = r;
	}

	@Override
	public boolean applicable(Map<String, Column> columnMap) {
		return left.applicable(columnMap) && right.applicable(columnMap);
	}

	public Column getLeft() {
		return left;
	}

	public Column getRight() {
		return right;
	}
}
