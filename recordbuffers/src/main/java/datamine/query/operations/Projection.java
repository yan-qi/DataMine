package datamine.query.operations;

import datamine.query.data.Column;
import datamine.query.data.Row;

public class Projection implements MapOperator {

    private Column[] columnList;

	public Projection(Column[] cols) {
		columnList = cols;
	}

	@Override
	public Column[] getColumns() {
		return columnList;
	}

	// Try to re-use this tuple instead of creating a new tuple each time;

	@Override
	public Row process(Row input) throws Exception {

		for (Column one : columnList) {
			if (one.getOperation() != null) {
				input.setColumnValue(one.getName(),
						one.applyOperation(input));
			}
		}

		return input;
	}

}