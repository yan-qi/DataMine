package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

import java.util.List;

public class NestedListMax extends NestedListOperation {

    public NestedListMax(Column target, Column predicate, boolean canCache, long limit) {
        super(target, predicate, canCache, limit);
    }

    @Override
    public Value apply(Row input) throws Exception {

        if (hasCache && cache.containsKey(getID())) {
            return cache.get(getID());
        }

        Value result = ValueUtils.nullValue;
        if (targetCol.getType() == ValueUtils.ListLongDataType) {
            long max = Long.MIN_VALUE;
            List<Long> vList = (List<Long>) targetCol.applyOperation(input).getValue();
            if (predicateCol == null) {
                for (long val : vList) {
                    if (val > max) {
                        max = val;
                    }
                }
            } else {
                for (int i=0; i<Math.min(vList.size(), limit); ++i) {
                    Value ret = predicateCol.applyOperation(input, i);
                    if (ValueUtils.isTrue(ret)) {
                        if (vList.get(i) > max) {
                            max = vList.get(i);
                        }
                    }
                }
            }
            result = new Value(max, ValueUtils.LongDataType);
        } else if (targetCol.getType() == ValueUtils.ListFloatDataType) {
            double max = Double.MIN_VALUE;
            List<Double> vList = (List<Double>) targetCol.applyOperation(input).getValue();
            if (predicateCol == null) {
                for (double val : vList) {
                    if (val > max) {
                        max = val;
                    }
                }
            } else {
                for (int i=0; i<Math.min(vList.size(), limit); ++i) {
                    Value ret = predicateCol.applyOperation(input, i);
                    if (ValueUtils.isTrue(ret)) {
                        if (vList.get(i) > max) {
                            max = vList.get(i);
                        }
                    }
                }
            }
            result = new Value(max, ValueUtils.DoubleDataType);
        } else {
            throw new Exception(String.format("Cannot apply MIN/MAX on data type of %s",
                targetCol.getType()));
        }


        if (hasCache) {
            cache.put(getID(), result);
        }
        return result;
    }

    @Override
    public int getResultValueType() {
        switch (targetCol.getType()) {
            case ValueUtils.ListLongDataType:
                return ValueUtils.LongDataType;
            case ValueUtils.ListFloatDataType:
                return ValueUtils.DoubleDataType;
            case ValueUtils.ListStringDataType:
                return ValueUtils.StringDataType;
        }
        throw new IllegalArgumentException(String.format("Cannot apply MIN/MAX on data type of %s",
            targetCol.getType()));
    }

    @Override
    public String getID() {
        return String.format("Nested max on col(%s) with predicate(%s)",
            targetCol.getID(),
            predicateCol == null ? "true" : predicateCol.getID());
    }
}
