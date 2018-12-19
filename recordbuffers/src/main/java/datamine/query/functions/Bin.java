package datamine.query.functions;

import datamine.query.data.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A function to create bins for play events.
 *
 * Particularly given a play event, it defines the bins based on
 *
 * (1) the start position of play
 * (2) the play duration, and
 * (3) the size of bin.
 *
 * A pair of index are calculated to identify the first and last
 * bins covered by the play event.
 *
 */
public class Bin implements Operation {

    /**
     * The column for start play position.
     */
    private final Column startCol;

    /**
     * The column for the play duration
     */
    private final Column lengthCol;

    /**
     * The column for the size of bin
     */
    private final Column binSizeCol;

    /**
     * The two columns for the min/max position.
     *
     * Note that these two columns are NOT used for now.
     */
    private Column minCol = null;
    private Column maxCol = null;

    public Bin(Column startCol, Column lengthCol, Column binSizeCol) throws Exception {
        this.startCol = startCol;
        this.lengthCol = lengthCol;
        this.binSizeCol = binSizeCol;
        init();
    }

    public Bin(Column startCol, Column lengthCol, Column binSizeCol, Column maxCol, Column minCol) throws Exception {
        this.startCol = startCol;
        this.lengthCol = lengthCol;
        this.binSizeCol = binSizeCol;
        this.minCol = minCol;
        this.maxCol = maxCol;
        init();
    }

    /**
     * Check if the input columns are of correct types for computation.
     *
     * @throws Exception an exception is thrown if the input is not of right type.
     */
    private void init() throws Exception {
        boolean allNumeric = ValueUtils.isNumbericType(startCol.getType()) &&
            ValueUtils.isNumbericType(lengthCol.getType()) &&
            ValueUtils.isNumbericType(binSizeCol.getType());

        if (maxCol != null) {
            allNumeric = allNumeric && ValueUtils.isNumbericType(maxCol.getType());
        }

        if (minCol != null) {
            allNumeric = allNumeric && ValueUtils.isNumbericType(minCol.getType());
        }

        if (!allNumeric) {
            throw new Exception("Cannot apply bin operation to non-numeric columns");
        }
    }

    @Override
    public Value apply(Row input) throws Exception {
        double offset = startCol.applyOperation(input).getDoubleValue();
        double bucketSize = binSizeCol.applyOperation(input).getDoubleValue();
        double duration = lengthCol.applyOperation(input).getDoubleValue();
        List<Long> endList = new ArrayList<>();
        endList.add((long) (offset / bucketSize));
        endList.add((long) ((offset + duration) / bucketSize));
        if (endList.get(0) > endList.get(1)) {
            throw new Exception(String.format("Duration (%d) should be positive", duration));
        }
        return new Value(endList, ValueUtils.ListLongDataType);
    }

    @Override
    public Value apply(Row input, int i) throws Exception {
        throw new Exception("Bin does not support list type operation.");
    }

    @Override
    public boolean applicable(Map<String, Column> columnMap) {
        boolean valid = startCol.applicable(columnMap)
            && lengthCol.applicable(columnMap) && binSizeCol.applicable(columnMap);

        if (minCol != null) {
            valid = valid && minCol.applicable(columnMap);
        }

        if (maxCol != null) {
            valid = valid && maxCol.applicable(columnMap);
        }

        return valid;
    }

    @Override
    public int getResultValueType() {
        return ValueUtils.ListLongDataType;
    }

    @Override
    public String getID() {
        return String.format("bin(%s, %s, %s, %s, %s)",
            startCol.getID(), lengthCol.getID(), binSizeCol.getID(),
            minCol != null ? minCol.getID() : "null",
            maxCol != null ? maxCol.getID() : "null");
    }
}
