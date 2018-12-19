package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtStr extends UnaryOperation {

	private static final Logger log = LoggerFactory.getLogger(ExtStr.class);

	private final int index;
	private final Pattern pattern;

	public ExtStr(Column strCol, String regx) throws Exception {
		super(strCol);
		this.pattern = Pattern.compile(regx);
		this.index = 0;

		if (strCol.getType() != ValueUtils.StringDataType || regx == null || regx.isEmpty()) {
			throw new Exception(SYNTAX_ERROR_LOG_PREFIX + "The input of ExtStr must be (String, String) - "
					+ strCol.getType() + " and " + regx);
		}
	}

	public ExtStr(Column strCol, String regx, int index) throws Exception {
		super(strCol);
		this.pattern = Pattern.compile(regx);
		this.index = index;

		if (strCol.getType() != ValueUtils.StringDataType || regx == null || regx.isEmpty()
		|| index < 0) {
			throw new Exception(SYNTAX_ERROR_LOG_PREFIX + "The input of ExtStr must be (String, String, Positive Int) - "
			+ strCol.getType() + " and " + regx  + " and " + index);
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		return getMatchStr(col.applyOperation(input).toString());
	}

	private Value getMatchStr(String txt) throws Exception {
		Matcher matcher = pattern.matcher(txt);
		boolean found = false;
		found = matcher.find();
		for (int i = 0; i < index; ++i) {
			found = matcher.find();
		}
		if (found) {
			return new Value(matcher.group(), ValueUtils.StringDataType);
		} else {
			return ValueUtils.nullValue;
		}
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		return getMatchStr(col.applyOperation(input, i).toString());
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.StringDataType;
	}

	@Override
	public String getID() {
		return "ExtStr(" + col.getID() + "," + this.pattern.pattern() + "," + index + ")";
	}

}
