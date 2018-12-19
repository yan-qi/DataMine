package datamine.query.operations;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.ValueUtils;

public class Filtering implements MapOperator {

	private Column filterExpression;

	private Column[] inputColumns;

	public Filtering(Column filter, Column[] input) {
        filterExpression = filter;
		inputColumns = input;
	}

	@Override
	public Row process(Row input) throws Exception {

		if (ValueUtils.isTrue(filterExpression.applyOperation(input))) {
			return input;
		} else {
			return null;
		}

	}

	public Column getCondition() {
		return filterExpression;
	}

	@Override
	public Column[] getColumns() {
		return inputColumns;
	}

}
