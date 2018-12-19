package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import datamine.utils.DataHasher;
import gnu.trove.set.hash.TLongHashSet;

import java.util.HashMap;
import java.util.Map;

/**
 * It counts the number of distinct values in a column.
 *
 * Note that it does only support counting on the column of profile ID.
 * In other words, it is NOT accurate when applying the function to any
 * column other than that to uniquely identify profiles.
 *
 * TODO add one more phase or HLL to deal with distinct counting other than profile_id
 */
public class CountDistinct extends AggOperation {

	private Value one;
	private Map<String, TLongHashSet> dist = new HashMap<>();

	public CountDistinct(Column c) throws Exception {
		super(c);
		one = new Value((long) 1, ValueUtils.LongDataType);
	}

	@Override
	public Value apply(Row input) throws Exception {
		return col.applyOperation(input);
	}

	@Override
	public void clear() {
		dist.clear();
	}

	@Override
	public Value apply(Value base, Value delta) throws Exception {
		if (base == null) {
			return delta;
		}
		return ValueUtils.plus(base, delta);
	}

	@Override
	public Value apply(String key, Value base, Value delta) throws Exception {
		TLongHashSet ids = dist.get(key);
		long id = DataHasher.hash(delta.toString());
		if (ids == null) {
			ids = new TLongHashSet();
			ids.add(id);
			dist.put(key, ids);
			if (base == null) {
				return one;
			}
			return ValueUtils.plus(base, one);
		} else {
			if (ids.contains(id)) {
				return base; // already exists, ignore
			}
			ids.add(id);
			if (base == null) {
				return one;
			}
			return ValueUtils.plus(base, one);
		}
	}

	@Override
	public String getID() {
		return "count(distinct " + col.getName() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.LongDataType;
	}

}
