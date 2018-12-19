package datamine.query.utils;

import datamine.query.data.Column;
import datamine.query.data.Row;
import datamine.query.data.Value;
import datamine.query.data.ValueUtils;
import datamine.query.operations.Aggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TupleAggregation implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(TupleAggregation.class);

    private final Aggregation agg;
    private final String delimiter;

    public TupleAggregation(Aggregation agg, String delimiter) {
        this.agg = agg;
        this.delimiter = delimiter;
    }

    public Row seqOp(Row base, String aggValStr) throws Exception {
        Row inc = Row.newInstance(aggValStr.split(delimiter), agg.getColumns());
        return agg.incrProcess(base, inc);
    }

    public Row comOp(Row base, Row inc) throws Exception {
        return agg.incrProcess(base, inc);
    }

    public static Tuple2<Value[], String> getOutputTuple(Column[] selectCols,
                                                         Column[] keyCols,
                                                         Column[] orderByCols,
                                                         String keyValueString,
                                                         Row aggRow,
                                                         String delimiter) {
        //0. calculate the columns in selectCols if necessary
        for (Column col : selectCols) {
            if (col.hasAggregation() && aggRow.getColumnValue(col.getID()) == null) {
                try {
                    Value val = col.applyOperation(aggRow);
                    aggRow.setColumnValue(col.getID(), val);
                } catch (Exception e) {
                    logger.error("Cannot calculate the value of " + col.getID() +
                        " from the tuple: " + aggRow);
                    throw new IllegalArgumentException(e);
                }
            }
        }

        //1. get all values for keys
        String[] keyVals = keyValueString.split(delimiter);

        int keyPos = 0;
        StringBuilder sb = new StringBuilder();
        Map<String, Value> selValMap = new HashMap<>();
        for (int i = 0; i < selectCols.length; ++i) {
            Column curCol = selectCols[i];
            if (keyPos < keyCols.length && curCol.equals(keyCols[keyPos])) {
                keyPos++;
                Value val = keyVals[keyPos].equals(ValueUtils.nullValue.toString())
                    ? ValueUtils.nullValue : new Value(keyVals[keyPos], curCol.getType());
                selValMap.put(curCol.getID(), val);
                sb.append(keyVals[keyPos]); // skip the left value (i.e., query id)
                sb.append(delimiter);
            } else {
                Value aggVal = aggRow.getColumnValue(curCol.getID());
                selValMap.put(curCol.getID(), aggVal);
                sb.append(aggVal);
                sb.append(delimiter);
            }
        }

        Value[] vals = new Value[1 + orderByCols.length];
        vals[0] = new Value(keyVals[0], ValueUtils.LongDataType); // the left is always query ID
        for (int i = 0; i < orderByCols.length; ++i) {
            Column col = orderByCols[i];
            vals[i + 1] = selValMap.get(col.getID());
        }

        return new Tuple2<>(vals, sb.toString());
    }
}