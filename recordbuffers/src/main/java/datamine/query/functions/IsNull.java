package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

public class IsNull extends UnaryOperation {

	private final boolean isNotNull;

	public IsNull(Column c) throws Exception {
		this(c, false);
	}

	public IsNull(Column c, boolean isNotNull) throws Exception {
        super(c);
        this.isNotNull = isNotNull;
	}

	@Override
	public Value apply(Row input) throws Exception {
		if (isNotNull && col.applyOperation(input).getType() != ValueUtils.NullDataType) {
			return ValueUtils.trueValue;
		}

		if (!isNotNull && col.applyOperation(input).getType() == ValueUtils.NullDataType) {
			return ValueUtils.trueValue;
		}

		return ValueUtils.falseValue;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
        if (isNotNull && col.applyOperation(input, i).getType() != ValueUtils.NullDataType) {
            return ValueUtils.trueValue;
        }

        if (!isNotNull && col.applyOperation(input, i).getType() == ValueUtils.NullDataType) {
            return ValueUtils.trueValue;
        }

        return ValueUtils.falseValue;
	}

	@Override
	public String getID() {
		return isNotNull ? "isNotNull (" : "isNull(" + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}
}