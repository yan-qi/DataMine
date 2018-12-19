package datamine.query.functions;


import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import gnu.trove.set.hash.THashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistDistinctCount2 extends AggOperation {

    public static final Logger logger = LoggerFactory.getLogger(HistDistinctCount2.class);

    private final Column valColumn;
    private final Map<String, THashSet<String>[]> distinctKeyValuesMap = new HashMap<>();
    private Value one;

	public HistDistinctCount2(Column binColumn, Column valColumn) throws Exception {
		super(binColumn);
		this.valColumn = valColumn;
		if (binColumn.getType() != ValueUtils.ListLongDataType) {
			throw new Exception("Cannot apply histogram approach to the data type: "
				+ binColumn.getType());
		}
	}

	// Init
	@Override
	public Value apply(Row input) throws Exception {
        String val = valColumn.applyOperation(input).getValue().toString();
        List<Long> ends = (List<Long>) col.applyOperation(input).getValue();
        String[] valStrArr = new String[(int) (ends.get(1) + 1)];
        valStrArr[ends.get(0).intValue()] = valStrArr[ends.get(1).intValue()] = val;
        one = new Value(valStrArr, ValueUtils.ArrayStringDataType);
        return one;
	}

	// Iter
	@Override
	public Value apply(Value base, Value delta) throws Exception {
		if (base == null) {
			return delta;
        }

        long[] baseArr = (long[]) base.getValue();
        long[] deltaArr = (long[]) delta.getValue();

        if (baseArr.length >= deltaArr.length) {
            for (int i = 0; i < deltaArr.length; ++i) {
                baseArr[i] += deltaArr[i];
            }
            return base;
        } else {
            for (int i = 0; i < baseArr.length; ++i) {
                deltaArr[i] += baseArr[i];
            }
            return delta;
        }
    }

    // Interface for aggregates over nested table
	@Override
	public Value apply(String key, Value base, Value delta) throws Exception {

        String[] deltaArr = (String[]) delta.getValue();

        if (base == null) {
            THashSet<String>[] valSetArray = new THashSet[deltaArr.length];
            long[] counts = new long[deltaArr.length];
            int numOfVals = 2;
            String val = null;
            for (int i=0; i<valSetArray.length; ++i) {
                if (deltaArr[i] != null) {
                    numOfVals--;
                    val = deltaArr[i];
                }

                if (val != null) {
                    valSetArray[i] = new THashSet<>();
                    valSetArray[i].add(val);
                    counts[i] = 1;
                }

                if (numOfVals <= 0) {
                    break; // may not be necessary as the delta is over.
                }
            }
            distinctKeyValuesMap.put(key, valSetArray);
            return new Value(counts, ValueUtils.ArrayLongDataType);
        }

        long[] baseArr = (long[]) base.getValue();
        String[] deltaValArr = (String[]) delta.getValue();
        baseArr = extendLongValArray(baseArr, deltaArr.length);
        THashSet<String>[] valSetArray = extendValSetArray(
            distinctKeyValuesMap.get(key), deltaValArr.length);

        int numOfVals = 2;
        String val = null;
        for (int i=0; i<deltaValArr.length; ++i) {
            if (deltaValArr[i] != null) {
                val = deltaValArr[i];
                numOfVals--;
            }

            if (val != null) {
                THashSet<String> curSet = valSetArray[i];
                if (curSet == null) {
                    curSet = new THashSet<>();
                }

                if (!curSet.contains(val)) {
                    baseArr[i]++;
                    curSet.add(val);
                    valSetArray[i] = curSet; // in case it was null
                }
            }

            if (numOfVals <= 0) {
                break;
            }
        }
        distinctKeyValuesMap.put(key, valSetArray);
        return new Value(baseArr, ValueUtils.ArrayLongDataType);
	}

    private THashSet<String>[] extendValSetArray(THashSet<String>[] valSetArray, int len) {
        if (valSetArray != null && valSetArray.length >= len) {
            return valSetArray;
        } else {
            THashSet<String>[] newValSetArray = new THashSet[len];
            if (valSetArray != null) {
                System.arraycopy(valSetArray, 0, newValSetArray, 0, valSetArray.length);
            }
            return newValSetArray;
        }
    }

    private long[] extendLongValArray(long[] valArray, int len) {
        if (valArray != null && valArray.length >= len) {
            return valArray;
        } else {
            long[] newValSetArray = new long[len];
            if (valArray != null) {
                System.arraycopy(valArray, 0, newValSetArray, 0, valArray.length);
            }
            return newValSetArray;
        }
    }

	@Override
	public String getID() {
		return "hist_count(" + col.getID() + " distinct " + valColumn.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.ArrayLongDataType;
	}

    @Override
    public void clear() {
        this.distinctKeyValuesMap.clear();
    }
}
