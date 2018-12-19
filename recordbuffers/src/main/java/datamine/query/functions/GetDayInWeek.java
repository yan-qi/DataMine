package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the day in a week given a timestamp.
 */
public class GetDayInWeek extends GetTemporalString {

	public GetDayInWeek(Column column, Column tzCol) throws Exception {
		super(column, tzCol,"E");
	}

	@Override
	public String getID() {
		return "dayInWeek(" + col.getID() + ")";
	}
}
