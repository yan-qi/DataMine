package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

import java.util.List;

public class Like extends BinaryOperation {

	boolean isStartWith = false;
    boolean isEndWith = false;
	boolean isContains = false;
	boolean isEqual = false;
	String simplePattern = "";
	String pattern = "";

	public Like(Column l, Column r) throws Exception {
		super(l, r);
		if (l.getType() != ValueUtils.StringDataType && l.getType() != ValueUtils.ListStringDataType) {
			throw new Exception(l.toString() + " is not a string or a list of strings.");
		}
		if (r.getType() != ValueUtils.StringDataType) {
			throw new Exception(r.toString() + " is not a string constant.");
		}
		if (!(r.getOperation() instanceof Constant)) {
			throw new Exception(r.toString() + " is not a string constant.");
		}

		String value = (String) r.getOperation().apply(Row.newInstance()).getValue();
		value = value.toLowerCase();
		pattern = ValueUtils.getRegEx(value);

		// Optimization;
		// Have to use regular expression if ? exists
		if (value.contains("?")) {
			return;
		}

		// case 1: ordinary string
		if (!value.contains("%")) {
			isEqual = true;
			simplePattern = value;
			return;
		}

		// Contains % wildcard
		// case 2: %abc% -- contains
		if (value.charAt(value.length() - 1) == '%' && value.charAt(0) == '%') {
			simplePattern = value.substring(1, value.length() - 1);
			if (!simplePattern.contains("%")) {
				isContains = true;
			}
		}
		// case 3: %abc -- endWith
		else if (value.charAt(0) == '%') {
			simplePattern = value.substring(1, value.length());
			if (!simplePattern.contains("%")) {
				isEndWith = true;
			}
		}
		// case 4: abc% -- startWith
		else if (value.charAt(value.length() - 1) == '%') {
			simplePattern = value.substring(0, value.length() - 1);
			if (!simplePattern.contains("%")) {
				isStartWith = true;
			}
		}
	}

	@Override
	public Value apply(Row input) throws Exception {

        if (left.getType() == ValueUtils.NullDataType || right.getType() == ValueUtils.NullDataType) {
            return ValueUtils.falseValue;
        }

        Value lVal = left.applyOperation(input);
        if (lVal.getType() == ValueUtils.NullDataType) {
            return ValueUtils.falseValue;
        }

        String l = (String) lVal.getValue();
		l = l.toLowerCase();
		if (isEqual) {
			return l.equals(simplePattern) ? ValueUtils.trueValue : ValueUtils.falseValue;
		}
		if (isStartWith) {
			return l.startsWith(simplePattern)
					? ValueUtils.trueValue
					: ValueUtils.falseValue;
		}
		if (isEndWith) {
			return l.endsWith(simplePattern)
					? ValueUtils.trueValue
					: ValueUtils.falseValue;
		}
		if (isContains) {
			return l.contains(simplePattern)
					? ValueUtils.trueValue
					: ValueUtils.falseValue;
		}
		return ValueUtils.like(l, pattern);
	}

	@Override
	public Value apply(Row input, int i) throws Exception {

		if (left.getType() == ValueUtils.NullDataType || right.getType() == ValueUtils.NullDataType) {
			return ValueUtils.falseValue;
		}

		List<String> lVals = (List<String>) left.applyOperation(input).getValue();

		String l = lVals.get(i);
        if (l == null) {
            return ValueUtils.falseValue;
        }
		l = l.toLowerCase();
		if (isEqual) {
			return l.equals(simplePattern) ? ValueUtils.trueValue : ValueUtils.falseValue;
		}
		if (isStartWith) {
			return l.startsWith(simplePattern)
				? ValueUtils.trueValue
				: ValueUtils.falseValue;
		}
		if (isEndWith) {
			return l.endsWith(simplePattern)
				? ValueUtils.trueValue
				: ValueUtils.falseValue;
		}
		if (isContains) {
			return l.contains(simplePattern)
				? ValueUtils.trueValue
				: ValueUtils.falseValue;
		}
		return ValueUtils.like(l, pattern);
	}

	@Override
	public String getID() {
		return left.getID() + " LIKE " + right.getID();
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}

}