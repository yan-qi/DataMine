package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class Not extends BooleanUnaryOperation {

	public Not(Column c) throws Exception {
        super(c, "~");
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.NOT(col.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return ValueUtils.NOT(col.applyOperation(input, i));
	}
}