package datamine.query.functions;

import datamine.query.data.Column;
import datamine.query.data.Operation;
import datamine.query.data.Row;
import datamine.query.data.Value;

import java.util.HashMap;
import java.util.Map;

public abstract class NestedListOperation implements Operation {

	final Column targetCol;
	final Column predicateCol;
	final long limit;

	final boolean hasCache;
    final Map<String, Value> cache = new HashMap<>();

    public void cleanCache() {
        cache.clear();
    }

    public NestedListOperation(Column target, Column predicate) {
        this(target, predicate, false, Long.MAX_VALUE);
    }

	public NestedListOperation(Column target, Column predicate, boolean hasCache, long limit) {
        this.targetCol = target;
        this.predicateCol = predicate;
		this.hasCache = hasCache;
		this.limit = limit;
	}

	@Override
	public boolean applicable(Map<String, Column> columnMap) {
		return targetCol.applicable(columnMap);
	}
	
	public Column getColumn() {
		return targetCol;
	}

	@Override
	public Value apply(Row input, int i) throws Exception {
		throw new Exception("NestedListOperation does not support list type operation.");
		// support a nesting function in a nesting function (recursively)
//		return apply(input);
	}
}
