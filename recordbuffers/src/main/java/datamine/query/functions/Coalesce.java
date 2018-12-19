package datamine.query.functions;


import datamine.query.data.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Define a function of determining the value from a list of columns.
 *
 * Note that the first column along the input list with the valid value is chosen as the result.
 *
 */
public class Coalesce implements Operation {

	List<Column> cols = new ArrayList<>();
	public Coalesce(List<Column> cols) throws Exception {
		this.cols = cols;
		// check the type: all columns must be all numeric or the same data type
		int colType = cols.get(0).getType();
		if (ValueUtils.isNumbericType(colType)) {
			for (int i = 1; i < cols.size(); ++i) {
				if (!ValueUtils.isNumbericType(cols.get(i).getType())) {
					throw new Exception("All columns in Coalesce must be numeric:" + cols);
				}
			}
		} else {
			for (int i = 1; i < cols.size(); ++i) {
				if (cols.get(1).getType() != colType) {
					throw new Exception("All columns in Coalesce must be of the same type:" + cols);
				}
			}
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		for (Column col : cols) {
			Value curVal = col.applyOperation(input);
			if (curVal != null && curVal != ValueUtils.nullValue) {
				return curVal;
			}
		}
		return ValueUtils.nullValue;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		for (Column col : cols) {
			Value curVal = col.applyOperation(input, i);
			if (curVal != ValueUtils.nullValue) {
				return curVal;
			}
		}
		return ValueUtils.nullValue;
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
		for (Column col : cols) {
			if (ValueUtils.isFloatingType(col.getType())) {
				return ValueUtils.DoubleDataType;
			} else if (ValueUtils.isNumbericType(col.getType())) {
				// it must be a LONG type
			} else {
				return col.getType();
			}
		}
		return ValueUtils.LongDataType;
	}

	@Override
	public String getID() {
		StringBuilder sb = new StringBuilder(cols.get(0).getID());
		for (int i = 1; i < cols.size(); ++i) {
			sb.append(",").append(cols.get(i).getID());
		}
		return "coalesce("+sb.toString()+")";
	}

}
