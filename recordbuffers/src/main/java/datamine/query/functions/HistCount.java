package datamine.query.functions;


import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HistCount extends AggOperation {

    public static final Logger logger = LoggerFactory.getLogger(HistCount.class);

    private Value one;

	public HistCount(Column column) throws Exception {
		super(column);
		if (column.getType() != ValueUtils.ListLongDataType) {
			throw new Exception("Cannot apply histogram approach to the data type: " + column.getType());
		}
	}

	// Init
	@Override
	public Value apply(Row input) throws Exception {
	    logger.info("Apply hist_count - " + System.currentTimeMillis());

        List<Long> ends = (List<Long>) col.applyOperation(input).getValue();
        one = new Value(new long[(int) (ends.get(1) + 1)], ValueUtils.ArrayLongDataType);
        add(ends.get(0), ends.get(1), one);

        return one;
	}

    private void add(long start, long end, Value base) {
        logger.debug("Increase the count from " + start + " to " + end);
	    long[] baseArr = (long[]) base.getValue();
        for (int i = (int)start; i <= end; ++i) {
            baseArr[i]++;
        }
    }

	// Iter
	@Override
	public Value apply(Value base, Value delta) throws Exception {
		if (base == null) {
			return delta;
        } else if (delta == null) {
		    return base;
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
		return apply(base, delta);
	}

	@Override
	public String getID() {
		return "hist_count(" + col.getID() + ")";
	}

	@Override
	public int getResultValueType() {
		return ValueUtils.ArrayLongDataType;
	}

}
