package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * Find out the maximum of all input. It is of aggregation function.
 */
public class Max extends ComparisonAggOperation {

    public Max(Column column) throws Exception {
        super(column);
    }

    @Override
    public Value apply(Value base, Value delta) throws Exception {

        if (base == null) {
            return delta;
        }

        if (!isString) {
            if (ValueUtils.isTrue(ValueUtils.greater(delta, base))) {
                return delta;
            } else {
                return base;
            }
        } else { // string
            if (base.getValue() == null) {
                return delta;
            }
            String baseString = (String) base.getValue();
            String deltaString = (String) delta.getValue();
            if (baseString.compareTo(deltaString) > 0) {
                return base;
            } else {
                return delta;
            }
        }
    }

    @Override
    public String getID() {
        return "max(" + col.getID() + ")";
    }
}
