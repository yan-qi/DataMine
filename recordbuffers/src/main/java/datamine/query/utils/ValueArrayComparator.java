package datamine.query.utils;

import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValueArrayComparator {

    private static final Logger logger = LoggerFactory.getLogger(ValueArrayComparator.class);

    public static int compare(Value[] x, Value[] y, Boolean[] isAscs) {
        try {
            for (int i = 0; i < isAscs.length; ++i) {
                Value l = x[i + 1]; // the left element is always the same query ID
                Value r = y[i + 1];
                if (ValueUtils.isTrue(ValueUtils.greater(l, r))) {
                    if (isAscs[i]) {
                        return 1;
                    } else {
                        return -1;
                    }
                } else if (ValueUtils.isTrue(ValueUtils.less(l, r))) {
                    if (isAscs[i]) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
            return 0;
        } catch (Exception ex) {
            logger.error("Error comparing " + x + " and " +
                y + " : " + ex.toString());
            return -1; // TODO is it correct?
        }
    }
}
