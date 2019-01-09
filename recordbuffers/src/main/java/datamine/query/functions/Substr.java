package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Substr extends BinaryOperation {

	private static final Logger log = LoggerFactory.getLogger(Substr.class);

	private final Column endPosCol;

	public Substr(Column strCol, Column startPosCol) throws Exception {
		super(strCol, startPosCol);
		this.endPosCol = null;

		if (strCol.getType() != ValueUtils.StringDataType
				|| !ValueUtils.isIntegerType(startPosCol.getType())) {
			throw new Exception(SYNTAX_ERROR_LOG_PREFIX + "The input of Substr must be (String, Int) - "
					+ strCol.getType() + " and " + startPosCol.getType());
		}
	}

	public Substr(Column strCol, Column startPosCol, Column endPosCol) throws Exception {
		super(strCol, startPosCol);
		this.endPosCol = endPosCol;

		if (strCol.getType() != ValueUtils.StringDataType
				|| !ValueUtils.isIntegerType(startPosCol.getType())
				|| endPosCol!=null && !ValueUtils.isIntegerType(endPosCol.getType())) {
			throw new Exception(SYNTAX_ERROR_LOG_PREFIX + "The input of Substr must be (String, Int, Int) - "
			+ strCol.getType() + " and " + startPosCol.getType()  + " and " + endPosCol.getType());
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		return getSubString(left.applyOperation(input).toString(), input);
	}

	private Value getSubString(String txt, Row input) throws Exception {
		long startPos = right.applyOperation(input).getLongValue();
		long endPos = endPosCol == null ? txt.length () : endPosCol.applyOperation(input).getLongValue();

		return new Value(txt.substring((int)startPos, (int)endPos));
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return getSubString(left.applyOperation(input, i).toString(), input);
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.StringDataType;
	}

	@Override
	public String getID() {
	    String endPosId = "";
	    if (endPosCol != null) {
	        endPosId = ", " + endPosCol.getID();
        }
		return "substr(" + left.getID() + "," + right.getID() + "," + endPosId + ")";
	}

}
