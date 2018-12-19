package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import gnu.trove.set.hash.TLongHashSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class In extends UnaryOperation {

	protected TLongHashSet vl = null;
	private Set<String> vs = null;

	public In(Column c) throws Exception {
		super(c);
	}

	public void setValueList(List<Value> p) throws Exception {
		Value o = p.get(0);

		if ((col.getType() == ValueUtils.StringDataType || col.getType() == ValueUtils.ListStringDataType) &&
				o.getType() == ValueUtils.StringDataType) {
			vs = new HashSet<>();
			for (Value x : p) {
				vs.add((String) x.getValue());
			}
			return;
		}

		if ((col.getType() == ValueUtils.LongDataType || col.getType() == ValueUtils.ListStringDataType) &&
				o.getType() == ValueUtils.LongDataType) {
			vl = new TLongHashSet();
			for (Value x : p) {
				Number y = (Number) x.getValue();
				vl.add(y.longValue());
			}
			return;
		}
		throw new Exception("Incompatible data type for IN operation:" +
				col.getType() + " .vs. " + o.getType());
	}

	@Override
	public Value apply(Row input) throws Exception {
		if (col.getType() == ValueUtils.StringDataType) {
			if (vs.contains(col.applyOperation(input).getValue())) {
				return ValueUtils.trueValue;
			} else {
				return ValueUtils.falseValue;
			}
		} else {
			Number x = (Number) col.applyOperation(input).getValue();
			if (vl.contains(x.longValue())) {
				return ValueUtils.trueValue;
			} else {
				return ValueUtils.falseValue;
			}
		}
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		List vals = (List) col.applyOperation(input).getValue();
		if (col.getType() == ValueUtils.ListStringDataType) {
			if (vs.contains(vals.get(i))) {
				return ValueUtils.trueValue;
			} else {
				return ValueUtils.falseValue;
			}
		} else {
			Number x = (Number) vals.get(i);
			if (vl.contains(x.longValue())) {
				return ValueUtils.trueValue;
			} else {
				return ValueUtils.falseValue;
			}
		}
	}

	@Override
	public String getID() {
		StringBuffer str = new StringBuffer();
		str.append(col.toString() + " IN(");
		if (col.getType() == ValueUtils.StringDataType || col.getType() == ValueUtils.ListStringDataType) {
			String[] abc = new String[vs.size()];
			abc = vs.toArray(abc);
			for (String s : abc) {
				str.append(s).append(",");
			}
		} else {
			long[] abc = new long[vl.size()];
			abc = vl.toArray();
			for (long s : abc) {
				str.append(Long.toString(s)).append(",");
			}
		}
		return str.append(")").toString();
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.BooleanDataType;
	}

	public String getName() {
		return col.toString();
	}
}
