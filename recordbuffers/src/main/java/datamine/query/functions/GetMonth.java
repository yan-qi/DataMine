package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the month in a year given a timestamp.
 */
public class GetMonth extends GetTemporalString {

	public GetMonth(Column column, Column tzCol) throws Exception {
		super(column, tzCol, "yyyy/MM");
	}

	@Override
	public String getID() {
		return "monthOf(" + col.getID() + ")";
	}
}
