package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;

/**
 * Define the behavior of equal operation.
 */
public class Equal extends BinaryOperation {

    public Equal(Column l, Column r) {
        super(l, r);
        /**
         * Check if the two column are compatible in terms of data type.
         * 1) both are not lists at the same time
         * 2) both are primitive, and of the same type
         * 3) both are primitive, and numeric
         */
        if ((ValueUtils.isList(l.getType()) && ValueUtils.isList(r.getType())) ||
                (!(ValueUtils.isList(l.getType()) || ValueUtils.isList(r.getType())) &&
                     (l.getType() != r.getType()) &&
                     !(ValueUtils.isNumbericType(l.getType()) && ValueUtils.isNumbericType(r.getType()))
                )
            ) {
            throw new IllegalArgumentException(
                l.getID() + " must be the same compatible type as " + r.getID());
        }

        if (ValueUtils.isList(r.getType())) {
            left = r;
            right = l;
        }
    }

    @Override
    public Value apply(Row input) throws Exception {
        return ValueUtils.equals(left.applyOperation(input),
            right.applyOperation(input));
    }

    @Override
    public Value apply(Row input, int i) throws Exception {
        return ValueUtils.equals(left.applyOperation(input, i),
            right.applyOperation(input, i));
    }

    @Override
    public String getID() {
        return left.getID() + "=" + right.getID();
    }

    @Override
    public int getResultValueType() {
        return ValueUtils.BooleanDataType;
    }
}
