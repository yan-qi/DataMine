package datamine.query.functions;

import datamine.query.data.Column;

/**
 * Get the calendar date given a timestamp.
 */
public class GetDate extends GetTemporalString {

	public GetDate(Column column, Column tzCol) throws Exception {
		super(column, tzCol, "yyyy/MM/dd");
	}

	@Override
	public String getID() {
		return "dateOf(" + col.getID() + ")";
	}

}
