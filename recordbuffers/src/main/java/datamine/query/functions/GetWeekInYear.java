package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the week in year given a timestamp.
 */
public class GetWeekInYear extends GetTemporalString {

	public GetWeekInYear(Column column, Column tzCol) throws Exception {
		super(column, tzCol,"w");
	}

	@Override
	public String getID() {
		return "weekInYear(" + col.getID() + ")";
	}
}
