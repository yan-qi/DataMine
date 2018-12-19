package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the calendar hour given a timestamp.
 */
public class GetHour extends GetTemporalString {

	public GetHour(Column column, Column tzCol) throws Exception {
		super(column, tzCol,"HH");
	}

	@Override
	public String getID() {
		return "hourOf(" + col.getID() + ")";
	}
}
