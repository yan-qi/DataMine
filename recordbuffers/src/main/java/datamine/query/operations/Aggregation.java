package datamine.query.operations;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.functions.*;

import java.io.Serializable;

public class Aggregation implements Serializable {

	// aggColumns supposes to be only aggregate columns;
    private Column[] aggColumns;

	public Aggregation(Column[] cols) {
		aggColumns = cols;
	}

	public Column[] getColumns() {
		return aggColumns;
	}

	// Used on the reduce side
	public Row incrProcess(Row base, Row delta) throws Exception {
		// Create a new Output tuple;
		if (base == null) {
			return Row.newInstance(delta);
		}

		for (Column one : aggColumns) {
			if (one.getOperation() != null &&
				one.getOperation() instanceof AggOperation) {
				Value oldV = base.getColumnValue(one.getName());
				Value deltaV = delta.getColumnValue(one.getName());
				AggOperation agg = (AggOperation) one.getOperation();
				base.setColumnValue(one.getName(), agg.apply(oldV, deltaV));
			}
		}
		return base;
	}

	public Row incrProcess(Row base, String[] fields) throws Exception {
		// Create a new Output tuple;
		if (base == null) {
			return Row.newInstance(fields, aggColumns);
		}
		for (int i=0; i<fields.length; i++) {
			Column one = aggColumns[i];
			if (one.getOperation() != null
				&& one.getOperation() instanceof AggOperation) {
				Value oldV = base.getColumnValue(one.getName());
				Value deltaV = new Value(fields[i], one.getType());
				AggOperation agg = (AggOperation) one.getOperation();
				base.setColumnValue(one.getName(), agg.apply(oldV, deltaV));
			}
		}
		return base;
	}

	// Aggregate on the Mapper side
    // aggregates over nested table
	public Row incrProcess(String key, Row base, Row delta)
			throws Exception {
		// Create a new Output tuple;
		if (base == null) {
			Row out = Row.newInstance(delta);
			for (Column one : aggColumns) {
				if (one.getOperation() instanceof CountDistinct ||
					one.getOperation() instanceof SumDistinct ||
					one.getOperation() instanceof HistDistinctCount ||
                    one.getOperation() instanceof HistDistinctCount2) {
					AggOperation agg = (AggOperation) one.getOperation();
					out.setColumnValue(
						one.getName(),
						agg.apply(key, null, delta.getColumnValue(one.getName())));
				}
			}
			return out;
		}

		for (Column one : aggColumns) {
			if (one.getOperation() != null &&
				one.getOperation() instanceof AggOperation) {
				Value oldV = base.getColumnValue(one.getName());
				Value deltaV = delta.getColumnValue(one.getName());
				AggOperation agg = (AggOperation) one.getOperation();
				base.setColumnValue(
				    one.getName(),
                    agg.apply(key, oldV, deltaV));
			}
		}
		return base;
	}


	// For distinct aggregates over nested tables; clear its cache;
	public void reset() throws Exception {
		for (Column one : aggColumns) {
			if (one.getOperation() instanceof CountDistinct ||
				one.getOperation() instanceof SumDistinct ||
				one.getOperation() instanceof HistDistinctCount ||
                one.getOperation() instanceof HistDistinctCount2 ||
				one.getOperation() instanceof PlaySessionCount) {
				AggOperation agg = (AggOperation) one.getOperation();
				agg.clear();
			}
		}
	}

}