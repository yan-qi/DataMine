package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class GreaterOrEqual extends Greater {

	public GreaterOrEqual(Column l, Column r) throws Exception {
		super(l, r);
	}

	@Override
	public Value apply(Row input) throws Exception {
		Value l = left.applyOperation(input);
		Value r = right.applyOperation(input);

		Value v = ValueUtils.greater(l, r);
		if (ValueUtils.isTrue(v)) {
			return v;
		}
		return ValueUtils.equals(l, r);
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		Value v = super.apply(input, i);
		if (ValueUtils.isTrue(v)) {
			return v;
		}
		return ValueUtils.equals(
			left.applyOperation(input, i),
			right.applyOperation(input, i));
	}

	@Override
	public String getID() {
		return left.getID() + ">=" + right.getID();
	}

}
