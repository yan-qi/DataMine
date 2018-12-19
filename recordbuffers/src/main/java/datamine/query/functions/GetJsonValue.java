package datamine.query.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GetJsonValue extends BinaryOperation {

	private static final Logger log = LoggerFactory.getLogger(GetJsonValue.class);

	public GetJsonValue(Column jsonStrCol, Column queryPathCol) throws Exception {
		super(jsonStrCol, queryPathCol);
		if (jsonStrCol.getType() != ValueUtils.StringDataType
				|| queryPathCol.getType() != ValueUtils.StringDataType) {
			throw new Exception(SYNTAX_ERROR_LOG_PREFIX + "The input of GetJsonValue must be two strings: "
			+ jsonStrCol.getType() + " and " + queryPathCol.getType());
		}
	}

	@Override
	public Value apply(Row input) throws Exception {
		Value jsonVal = left.applyOperation(input);
		Value queryVal = right.applyOperation(input);

		return getQueryResult(jsonVal.toString(), queryVal.toString());
	}

	private Value getQueryResult(String jsonStr, String query) {
		String[] queryPath = query.split("/");
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(jsonStr);
			for (int i = 0; i < queryPath.length; ++i) {
				node = node.get(queryPath[i]);
			}
			return new Value(node.textValue(), ValueUtils.StringDataType);
		} catch (IOException e) {
			log.debug("Cannot parse a JSON string: " + jsonStr);
		}
		return ValueUtils.nullValue;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		Value jsonVal = left.applyOperation(input, i);
		Value queryVal = right.applyOperation(input, i);

		return getQueryResult(jsonVal.toString(), queryVal.toString());
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.StringDataType;
	}

	@Override
	public String getID() {
		return "json_val(" + left.getID() + ", " + right.getID() + ")";
	}

}
