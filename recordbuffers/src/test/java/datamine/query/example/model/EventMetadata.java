package datamine.query.example.model;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.FieldTypeFactory;
import datamine.storage.idl.type.PrimitiveType;


/**
 * DO Not CHANGE! Auto-generated code
 */
public enum EventMetadata implements RecordMetadataInterface {

	EVENT_TIME((short)1, "event_time", FieldTypeFactory.getPrimitiveType(PrimitiveType.INT64), true, null, false, true, false, false),
	EVENT_ID((short)2, "event_id", FieldTypeFactory.getPrimitiveType(PrimitiveType.STRING), true, null, false, false, false, false),
	DURATION((short)3, "duration", FieldTypeFactory.getPrimitiveType(PrimitiveType.DOUBLE), true, null, false, false, false, false),
	POST_TIME((short)4, "post_time", FieldTypeFactory.getPrimitiveType(PrimitiveType.INT64), true, null, false, false, false, false),
	URL((short)5, "url", FieldTypeFactory.getPrimitiveType(PrimitiveType.STRING), true, null, false, false, false, false),
	START((short)6, "start", FieldTypeFactory.getPrimitiveType(PrimitiveType.DOUBLE), true, null, false, false, false, false),
	BASIC_ATTRS((short)7, "basic_attrs", FieldTypeFactory.getGroupType(BasicMetadata.class), true, null, false, false, false, false),
	ATTR_LIST((short)8, "attr_list", FieldTypeFactory.getListType(FieldTypeFactory.getGroupType(BasicMetadata.class)), true, null, false, false, false, false),
;

	static final short version = 1;
	static final String name = "event";
	private Field field;

	private EventMetadata(short id, String name, FieldType type,
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


