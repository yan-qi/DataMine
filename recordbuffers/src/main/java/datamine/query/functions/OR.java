package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class OR extends BooleanBinaryOperation {

	public OR(Column l, Column r) throws Exception {
        super(l, r, "or");
		// move the selective operation ahead
		if (r.getOperation() instanceof Equal || r.getOperation() instanceof In) {
			Column temp = left;
			left = right;
			right = temp;
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		Value l = left.applyOperation(input);
		if (ValueUtils.isTrue(l)) {
			return l;
		}
		return right.applyOperation(input);
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		Value l = left.applyOperation(input, i);
		if (ValueUtils.isTrue(l)) {
			return l;
		}
		return right.applyOperation(input, i);
	}
}