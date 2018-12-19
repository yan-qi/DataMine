package datamine.query.functions;


import datamine.query.data.Column;
import datamine.query.data.Operation;
import datamine.query.data.Row;
import datamine.query.data.Value;

import java.util.Map;

/**
 * This is a base class for aggregation functions.
 */
public abstract class AggOperation implements Operation {

    /**
     * The column to which the aggregation is applied.
     */
	protected Column col;

	public AggOperation(Column input) {
		col = input;
	}

    /**
     * The aggregation running at the reducing phase.
     * @param base the value of one reducer
     * @param delta the value of the other reducer
     * @return the value of aggregating two input values
     * @throws Exception an exception can be thrown if fails.
     */
	public abstract Value apply(Value base, Value delta) throws Exception;

    /**
     * The aggregation running at the mapping phase.
     * @param key the value of aggregation key
     * @param base the value of one row
     * @param delta the value of the other row
     * @return the value of aggregating two input rows
     * @throws Exception an exception can be thrown if fails.
     */
	public abstract Value apply(String key, Value base, Value delta) throws Exception;

    /**
     * Check if the value can be computed from a set of columns.
     * @param output a set of basic columns based on which the target column can be derived
     * @return true if the value can be derived, false otherwise.
     */
	@Override
	public boolean applicable(Map<String, Column> output) {
		return false;
	}

	public Column getColumn() {
		return col;
	}

    @Override
    public Value apply(Row input, int i) throws Exception {
        throw new Exception("Aggregate function does not support list type operation.");
    }

	public void clear() throws Exception {
    	throw new Exception("Reset function is used mostly for distinct operations");
	}
}
