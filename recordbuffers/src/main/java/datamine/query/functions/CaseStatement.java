package datamine.query.functions;


import datamine.query.data.*;

import java.util.Map;

/**
 * Define the statement for case-when-else-end. A common use case can be
 * explained with an example below.
 *
 * case when a > 100 then 'a>100' when a > 10 then 'a>10' else 'a<=10' end
 *
 */
public class CaseStatement implements Operation {

	private Column[] cases;
	private Column[] output;

    /**
     * There are two array-type input parameters.
     *
     * Note that these two arrays are correlated. For example, the ith elements of
     * the two arrays describe the ith condition checking in the statement.
     *
     * The second column can have one more element at the end, which is for the 'ELSE'
     * clause.
     *
     * @param c an array of columns for condition checking, e.g., col1 > 100
     * @param o an array of columns for output, e.g., 'col1>100'
     * @throws Exception an exception is thrown if the input are not qualified.
     */
	public CaseStatement(Column[] c, Column[] o) throws Exception {
		if (c == null || o == null || c.length > o.length) {
			throw new IllegalArgumentException(
				"There should be one more outputs than case expressions.");
		}
		cases = c;
		output = o;
	}

	@Override
	public Value apply(Row input) throws Exception {
		int i;
		//1. iterate the input array, to evaluate when-then clauses
		for (i = 0; i < cases.length; i++) {
			if (ValueUtils.isTrue(cases[i].applyOperation(input))) {
				return output[i].applyOperation(input);
			}
		}
		//2. evaluate the ELSE part if necessary
		if (output.length > cases.length) {
			return output[i].applyOperation(input);
		}

		return ValueUtils.nullValue;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		throw new Exception("Case does not support list type operation.");
	}

	@Override
	public String getID() {
		StringBuffer str = new StringBuffer();
		str.append("case ");
		int i = 0;
		for (i = 0; i < cases.length; i++) {
			str.append(" when ").append(cases[i].getID()).append(" then ")
					.append(output[i].getID());
		}
		if (output.length > cases.length) {
			str.append(" else ").append(output[i].getID()).append(" end");
		}
		return str.toString();
	}

	@Override
	public int getResultValueType() {

		// String if string;
		if (output[0].getType() == ValueUtils.StringDataType) {
			return ValueUtils.StringDataType;
		}

		// Float if any output float
		for (Column c : output) {
			if (c.getType() == ValueUtils.FloatDataType) {
				return ValueUtils.FloatDataType;
			}
		}
		// We do not have boolean output from Case expression
		return ValueUtils.LongDataType;
	}

	@Override
	public boolean applicable(Map<String, Column> columnMap) {
		for (Column c : cases) {
			if (!c.applicable(columnMap)) {
				return false;
			}
		}
		for (Column c : output) {
			if (!c.applicable(columnMap)) {
				return false;
			}
		}
		return true;
	}

	public Column[] getCases() {
		return cases;
	}
	
	public Column[] getOutput() {
		return output;
	}
}
