package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

import java.util.List;

public class NestedListCount extends NestedListOperation {

    public NestedListCount(Column target, Column predicate, boolean canCache, long limit) {
        super(target, predicate, canCache, limit);
    }

    @Override
    public Value apply(Row input) throws Exception {

        if (hasCache && cache.containsKey(getID())) {
            return cache.get(getID());
        }

        int count = 0;
        List targetList = (List) targetCol.applyOperation(input).getValue();
        if (predicateCol == null) {
            count = targetList.size();
        } else {
            for (int i=0; i< Math.min(targetList.size(), limit); ++i) {
                Value ret = predicateCol.applyOperation(input, i);
                count += ValueUtils.isTrue(ret) ? 1 : 0;
            }
        }
        Value result = new Value(count, ValueUtils.LongDataType);
        if (hasCache) {
            cache.put(getID(), result);
        }
        return result;
    }

    @Override
    public int getResultValueType() {
        return ValueUtils.LongDataType;
    }

    @Override
    public String getID() {
        return String.format("Nested Count on col(%s) with predicate(%s)",
            targetCol.getID(),
            predicateCol == null ? "true" : predicateCol.getID());
    }
}
