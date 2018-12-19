package datamine.query.data;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.type.CollectionFieldType;
import datamine.storage.idl.type.CollectionType;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.GroupFieldType;
import datamine.storage.recordbuffers.Record;
import datamine.storage.recordbuffers.RecordBufferMeta;
import datamine.storage.recordbuffers.idl.DerivedFieldsInRecord;
import org.apache.commons.lang.StringUtils;

import java.util.*;

public class GroupFlatten {

    // input parameters
    private final String groupFieldName;
    private final Object groupFieldValue;
    private final Set<String> flatteningFields;
    private final Map<String, Field> fieldMap;
    private final Map<String, RecordBufferMeta> metaDictionary;
    private final Map<Field, DerivedFieldsInRecord> derivedFieldsInRecordMap;

    // helping variables
    private List<Object> recordList;
    private Record curRecord;
    private GroupFlatten subFlatten = null;
    private int cursor = 0;
    private int size = 0;

    private Set<String> firstLevelSimpleFieldNameMap = new HashSet<>();
    private Set<String> firstLevelGroupFieldNameMap = new HashSet<>();
    private Map<String, Field> subGroupFieldMap = new HashMap<>();
    private String subGroupFieldName = null;

    /**
     * Constructor.
     * @param groupFieldValue the value to be flattened
     * @param groupFieldName a full field name for group type, e.g., a.b.c
     * @param fieldMap a map of full field name to its corresponding field instance
     * @param metaDictionary a dictionary for lookup of table column name to its metadata
     * @throws Exception
     */
    public GroupFlatten(Object groupFieldValue,
                        String groupFieldName,
                        Set<String> flatteningFields,
                        Map<String, Field> fieldMap,
                        Map<String, RecordBufferMeta> metaDictionary,
                        Map<Field, DerivedFieldsInRecord> derivedFieldsInRecordMap) throws Exception {
        this.groupFieldValue = groupFieldValue;
        this.groupFieldName = groupFieldName;
        this.flatteningFields = flatteningFields;
        this.fieldMap = fieldMap;
        this.metaDictionary = metaDictionary;
        this.derivedFieldsInRecordMap = derivedFieldsInRecordMap;
        setup();
    }

    private void setup() {
        //1. determine the sub-groups (or the group at which it is flattened)
        for (String cur : flatteningFields) {
            int pos = cur.lastIndexOf('.');
            if (pos > 0 && cur.substring(0, pos).equals(groupFieldName)) {
                subGroupFieldName = cur;
                break;
            }
        }

        //2. validate the field names
        for (String curField : fieldMap.keySet()) {
            String[] names = curField.split("\\.");
            if (names.length < 2) {
                throw new IllegalArgumentException(
                        curField + " should be in the format of group.field."
                );
            }

            if (!curField.contains(groupFieldName)) {
                throw new IllegalArgumentException(
                        curField + " should have a group as " + groupFieldName);
            }

            if (curField.endsWith(groupFieldName)) {
                throw new IllegalArgumentException(
                        curField + " should not have a leaf group as " + groupFieldName
                );
            }

            String curGroupFieldName = curField.substring(0, curField.lastIndexOf('.'));
            if (curGroupFieldName.equals(groupFieldName)) {
                firstLevelSimpleFieldNameMap.add(curField);
            } else if (StringUtils.isNotBlank(subGroupFieldName)
                    && curField.startsWith(subGroupFieldName)) {
                subGroupFieldMap.put(
                        curField,
                        fieldMap.get(curField));
            } else {
                // dealing with the record
                firstLevelGroupFieldNameMap.add(curField);

                // todo deal with the non-flattening lists
            }
//            else {
//                StringBuilder sb = new StringBuilder();
//                for (int i = 0; i < names.length - 1; ++i) {
//                    sb.append(names[i]);
//                    if (sb.toString().equals(groupFieldName)) {
//                        subGroupFieldName = sb.toString() + "." + names[i + 1];
//                        subGroupFieldMap.put(
//                            curField,
//                            fieldMap.get(curField));
//                        break;
//                    }
//                    sb.append(".");
//                }
//            }
        }

        //2. decide size
        if (groupFieldValue instanceof List) {
            recordList = (List) groupFieldValue;
            size = ((List) groupFieldValue).size();
            if (size > 0) {
                curRecord = (Record) recordList.get(0);
            }
        } else if (groupFieldValue instanceof Record){
            curRecord = (Record) groupFieldValue;
            size = 1;
        } else {
            throw new IllegalArgumentException("The input of groupFieldValue should be "
                    + "either an instance of Record, or a List of Records");
        }
    }

    public boolean hasNext() {
        return (cursor < size) || (subFlatten != null && subFlatten.hasNext());
    }

    public Row next(Row t) throws Exception {

        //1. check the element from the sub-group left
        if (subFlatten != null && subFlatten.hasNext()) {
            return subFlatten.next(t);
        }

        //2. ensure no out-of-boundary reading
        if (cursor >= size) {
            throw new IllegalArgumentException("The cursor " + cursor
                    + " is out of range of " + size);
        }

        //3. fetch the field values at the left level
        for (String firstLevelFieldFullName : firstLevelSimpleFieldNameMap) {
            Field firstLevelField = fieldMap.get(firstLevelFieldFullName);
            Object val = null;
            if (firstLevelField.isDerived()) {
                if (derivedFieldsInRecordMap.containsKey(firstLevelField)) {
                    val = derivedFieldsInRecordMap.get(firstLevelField).getValue(firstLevelField, curRecord);
                } else {
                    val = firstLevelField.getDefaultValue();
                }
            } else {
                val = curRecord.getValue(firstLevelField);
            }
            t.setColumnValue(
                    firstLevelFieldFullName,
                    val != null ? new Value(val, firstLevelField.getType().getID()) : ValueUtils.nullValue
            );
        }

        //4. get values in the group of the 1st level
        Map<String, Record> recordValMap = new HashMap<>();
        for (String firstLevelFieldFullName : firstLevelGroupFieldNameMap) {
            String recordFullName = firstLevelFieldFullName.substring(0,
                    firstLevelFieldFullName.lastIndexOf('.'));

            if (!recordValMap.containsKey(recordFullName)) {
                // fixme there can be more than one levels embedded
                int pos = recordFullName.lastIndexOf('.');
                String parentTableName = recordFullName.substring(0, pos);
                String recordTableName = recordFullName.substring(pos + 1);
                RecordMetadataInterface tableMeta = (RecordMetadataInterface)
                        metaDictionary.get(parentTableName).getFieldMeta(recordTableName);
                Field recordField = tableMeta.getField();
                Record recordVal = (Record) curRecord.getValue(recordField);
                recordValMap.put(recordFullName, recordVal);
            }

            Field field = fieldMap.get(firstLevelFieldFullName);
            Record recordVal = recordValMap.get(recordFullName);
            Object val = null;
            if (recordVal != null) {
                if (field.isDerived()) {
                    if (derivedFieldsInRecordMap.containsKey(field)) {
                        val = derivedFieldsInRecordMap.get(field).getValue(field, recordVal);
                    } else {
                        val = field.getDefaultValue();
                    }
                } else {
                    val = recordVal.getValue(field);
                }
            }
            t.setColumnValue(
                    firstLevelFieldFullName,
                    val != null ? new Value(val, field.getType().getID()) : ValueUtils.nullValue
            );
        }

        //5. consider sub-group
        if (StringUtils.isNotBlank(subGroupFieldName)) {
            String simpleFieldName = subGroupFieldName.substring(subGroupFieldName.lastIndexOf('.') + 1);
            RecordMetadataInterface subGroupMeta = (RecordMetadataInterface) metaDictionary
                    .get(groupFieldName)
                    .getFieldMeta(simpleFieldName);

            Field subGroupField = subGroupMeta.getField();
            FieldType subGroupFieldType = subGroupField.getType();

            Object groupValue = curRecord.getValue(subGroupField);
            List<Object> subGroupRecordList = new ArrayList<>();
            if (subGroupFieldType instanceof GroupFieldType) {

                subGroupRecordList.add(groupValue);

            } else if (subGroupFieldType instanceof CollectionFieldType) {
                CollectionType collectionType =
                        ((CollectionFieldType) subGroupFieldType).getCollectionType();
                FieldType elementType = ((CollectionFieldType) subGroupFieldType).getElementType();

                if (!collectionType.equals(CollectionType.LIST)) {
                    throw new IllegalArgumentException(
                            "Do not support non-list-type nesting");
                }

                if (!(elementType instanceof GroupFieldType)) {
                    throw new IllegalArgumentException(
                            "Do not support primitive-list-type nesting");
                }

                subGroupRecordList = (List<Object>) groupValue;
            }

            //crate subFlatten instance
            if (subGroupRecordList != null && !subGroupRecordList.isEmpty()) {
                subFlatten = new GroupFlatten(
                        subGroupRecordList, subGroupFieldName, flatteningFields,
                        subGroupFieldMap, metaDictionary, derivedFieldsInRecordMap);

                t = subFlatten.next(t);
            }
        }

        //5. get the next record
        if (++cursor < size) {
            curRecord = (Record) recordList.get(cursor);
        }
        return t;
    }
}
