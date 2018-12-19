package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the calendar year given a timestamp.
 */
public class GetYear extends GetTemporalString {

	public GetYear(Column column, Column tzCol) throws Exception {
		super(column, tzCol,"yyyy");
	}

	@Override
	public String getID() {
		return "yearOf(" + col.getID() + ")";
	}

}
