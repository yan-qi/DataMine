package datamine.query.operations;

import datamine.query.data.Column;
import datamine.query.data.Row;

import java.io.Serializable;

public interface MapOperator extends Serializable {

	Row process(Row input) throws Exception;

	Column[] getColumns();

}