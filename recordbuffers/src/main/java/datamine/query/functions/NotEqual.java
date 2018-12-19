package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class NotEqual extends BinaryOperation {

	public NotEqual(Column l, Column r) throws Exception {
        super(l, r);
		if ((ValueUtils.isList(l.getType()) && ValueUtils.isList(r.getType()))
			||
			(!(ValueUtils.isList(l.getType()) || ValueUtils.isList(r.getType()))
				&&
				(l.getType() != r.getType())
				&&
				!(ValueUtils.isNumbericType(l.getType()) && ValueUtils.isNumbericType(r.getType())))) {
			throw new IllegalArgumentException(
				l.getID() + " must be the same numeric type as " + r.getID());
		}

		if (ValueUtils.isList(r.getType())) {
			left = r;
			right = l;
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.notequals(left.applyOperation(input),
				right.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return ValueUtils.notequals(left.applyOperation(input, i),
			right.applyOperation(input, i));
	}

	@Override
	public String getID() {
		return left.getID() + "<>" + right.getID();
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}
}