package datamine.query.functions;


import datamine.query.data.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Define a function of concatenating the values of a list of columns into a string.
 */
public class Concat implements Operation {

	List<Column> cols = new ArrayList<>();
	public Concat(List<Column> cols) throws Exception {
		this.cols = cols;
	}

	@Override
	public Value apply(Row input) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (Column col : cols) {
			Value curVal = col.applyOperation(input);
			sb.append(curVal == null ? "null" : curVal.toString());
		}
		return new Value(sb.toString(), ValueUtils.StringDataType);
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (Column col : cols) {
			sb.append(col.applyOperation(input, i).toString());
		}
		return new Value(sb.toString(), ValueUtils.StringDataType);
	}

	@Override
	public boolean applicable(Map<String, Column> columnMap) {
		for (Column col : cols) {
			if (!col.applicable(columnMap)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.StringDataType;
	}

	@Override
	public String getID() {
		StringBuilder sb = new StringBuilder(cols.get(0).getID());
		for (int i = 1; i < cols.size(); ++i) {
			sb.append(",").append(cols.get(i).getID());
		}
		return "concat("+sb.toString()+")";
	}

}
