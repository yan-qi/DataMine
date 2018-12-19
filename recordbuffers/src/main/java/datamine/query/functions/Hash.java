package datamine.query.functions;


import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class Hash extends UnaryOperation {

	public Hash(Column c) throws Exception {
		super(c);
		if (c.getType() != ValueUtils.StringDataType) {
			throw new Exception(c.toString() + " is not a string.");
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.hashFunc(col.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		throw new Exception("Addition does not support list type operation.");
	}

	@Override
	public String getID() {
		return "hash(" + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.LongDataType;
	}

}
