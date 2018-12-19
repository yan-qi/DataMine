package datamine.query.example.model;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.FieldTypeFactory;
import datamine.storage.idl.type.PrimitiveType;


/**
 * DO Not CHANGE! Auto-generated code
 */
public enum ProfileMetadata implements RecordMetadataInterface {

	PROFILE_ID((short)1, "profile_id", FieldTypeFactory.getPrimitiveType(PrimitiveType.STRING), true, null, false, false, false, false),
	CREATION_TIME((short)2, "creation_time", FieldTypeFactory.getPrimitiveType(PrimitiveType.INT64), true, null, false, false, false, false),
	LAST_MODIFIED_TIME((short)3, "last_modified_time", FieldTypeFactory.getPrimitiveType(PrimitiveType.INT64), true, null, false, false, false, false),
	EVENTS((short)4, "events", FieldTypeFactory.getListType(FieldTypeFactory.getGroupType(EventMetadata.class)), false, null, false, false, false, false),
;

	static final short version = 1;
	static final String name = "profile";
	private Field field;

	private ProfileMetadata(short id, String name, FieldType type,
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


