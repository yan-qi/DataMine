package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NestedListCountDistinct extends NestedListOperation {

    public static final Logger LOG = LoggerFactory.getLogger(NestedListCountDistinct.class);
    private Set<String> distincts;

    public NestedListCountDistinct(Column target, Column predicate, boolean canCache, long limit) {
        super(target, predicate, canCache, limit);
    }

    @Override
    public Value apply(Row input) throws Exception {

        if (hasCache && cache.containsKey(getID())) {
            return cache.get(getID());
        }

        List targetList = (List) targetCol.applyOperation(input).getValue();
        distincts = new HashSet<>(targetList.size());
        if (predicateCol == null) {
            distincts.addAll(targetList);
        } else {
            for (int i=0; i < Math.min(targetList.size(), limit); ++i) {
                Value ret = predicateCol.applyOperation(input, i);
                if (ValueUtils.isTrue(ret)) {
                    String val = targetList.get(i) == null ? "null" : targetList.get(i).toString();
                    distincts.add(val);
                }
            }
        }

        Value result = new Value(distincts.size(), ValueUtils.LongDataType);
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
        return String.format("Nested Count Distinct on col(%s) with predicate(%s)",
            targetCol.getID(),
            predicateCol == null ? "true" : predicateCol.getID());
    }
}
