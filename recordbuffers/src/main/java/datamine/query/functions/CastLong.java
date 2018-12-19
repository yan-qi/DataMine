package datamine.query.functions;


import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * Define a function of casting a numeric value into a LONG.
 */
public class CastLong extends UnaryOperation {

	public CastLong(Column c) throws Exception {
		super(c);
		if (!ValueUtils.isNumbericType(c.getType()) && c.getType() != ValueUtils.StringDataType) {
			throw new Exception(c.toString() + " cannot be cast into a LONG.");
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		return ValueUtils.castLong(col.applyOperation(input));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
	    Value l = this.col.applyOperation(input, i);
	    return ValueUtils.castLong(l);
	}

	@Override
	public String getID() {
		return "tolong(" + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.LongDataType;
	}

}
