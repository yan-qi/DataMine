package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import gnu.trove.set.hash.TLongHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SumDistinct extends AggOperation {

	private Map<String, TLongHashSet> distinctIds = new HashMap<>();
    private Map<String, Set<Float>> distinctValues = new HashMap<>();
	private boolean isFloat = false;

	public SumDistinct(Column c) throws Exception {
		super(c);
		if (!ValueUtils.isNumbericType(c.getType())) {
			throw new Exception(c.getID() + " must be a number.");
		}
		if (c.getType() == ValueUtils.FloatDataType || c.getType() == ValueUtils.DoubleDataType) {
			isFloat = true;
		}
	}

	// Init (combiner, reducer)
	@Override
	public Value apply(Row input) throws Exception {
		return col.applyOperation(input);
	}

	@Override
	public void clear() {
		distinctIds.clear();
		distinctValues.clear();
	}

	// Iter (combiner, reducer)
	@Override
	public Value apply(Value base, Value delta) throws Exception {
		if (base == null) {
			return delta;
		}
		return ValueUtils.plus(base, delta);
	}

	// Init Iter in mapper
	@Override
	public Value apply(String key, Value base, Value delta) throws Exception {
		if (!isFloat) {
			TLongHashSet ids = distinctIds.get(key);
			Number id = (Number) delta.getValue();
			if (ids == null) {
				ids = new TLongHashSet();
				ids.add(id.longValue());
				distinctIds.put(key, ids);
				return apply(base, delta);
			} else {
				if (ids.contains(id.longValue())) {
					return base; // already exists, ignore
				}
				ids.add(id.longValue());
				return apply(base, delta);
			}
		} else {
			Set<Float> ids = distinctValues.get(key);
			Number id = (Number) delta.getValue();
			if (ids == null) {
				ids = new HashSet<Float>();
				ids.add(id.floatValue());
				distinctValues.put(key, ids);
				return apply(base, delta);
			} else {
				if (ids.contains(id.floatValue())) {
					return base; // already exists, ignore
				}
				ids.add(id.floatValue());
				return apply(base, delta);
			}
		}
	}

	@Override
	public String getID() {
		return "sum(distinct " + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		if (isFloat) {
			return ValueUtils.FloatDataType;
		} else {
			return ValueUtils.LongDataType;
		}
	}
}