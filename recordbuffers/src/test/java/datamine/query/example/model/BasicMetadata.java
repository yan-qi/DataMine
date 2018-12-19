package datamine.query.example.model;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.FieldTypeFactory;
import datamine.storage.idl.type.PrimitiveType;


/**
 * DO Not CHANGE! Auto-generated code
 */
public enum BasicMetadata implements RecordMetadataInterface {

	EVENT_TIME((short)1, "event_time", FieldTypeFactory.getPrimitiveType(PrimitiveType.INT64), true, null, false, false, false, false),
;

	static final short version = 1;
	static final String name = "basic";
	private Field field;

	private BasicMetadata(short id, String name, FieldType type,
		boolean isRequired, Object defaultValue, boolean isDesSorted, 
		boolean isAscSorted, boolean isFrequentlyUsed, boolean isDerived) {
		field = Field.newBuilder(id, name, type).
				withDefaultValue(defaultValue).
				isRequired(isRequired).
				isDesSorted(isDesSorted).
				isAscSorted(isAscSorted).
				isFrequentlyUsed(isFrequentlyUsed).
				isDerived(isDerived).
				build();
	}

	@Override
	public Field getField() {
		return field; 
	}

	@Override
	public short getVersion() { 
		return version; 
	}

	@Override
	public String getTableName() { 
		return name; 
	}
}


