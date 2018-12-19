package datamine.query.data;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.idl.type.CollectionFieldType;
import datamine.storage.idl.type.FieldType;
import datamine.storage.idl.type.PrimitiveType;
import datamine.storage.recordbuffers.Record;
import datamine.storage.recordbuffers.RecordBufferMeta;
import datamine.storage.recordbuffers.idl.DerivedFieldsInRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Only one level of flattening is supported.
 */
public class GroupFieldsFlatten {

    // input parameters
    private final Record groupFieldValue;
    private final Map<String, Field> fieldMap;
    private final Map<String, RecordBufferMeta> metaDictionary;
    private final long minTs;
    private final long maxTs;
    private final String timeStampFieldSimpleName;
    private final Map<Field, DerivedFieldsInRecord> derivedFieldsInRecordMap;

    /**
     * Constructor.
     * @param groupFieldValue the value to be flattened
     * @param fieldMap a map of full field name to its corresponding field instance
     * @param metaDictionary a dictionary for lookup of table column name to its metadata
     */
    public GroupFieldsFlatten(Record groupFieldValue,
                              long minTs,
                              long maxTs,
                              String timeStampFieldSimpleName,
                              Map<String, Field> fieldMap,
                              Map<String, RecordBufferMeta> metaDictionary,
                              Map<Field, DerivedFieldsInRecord> derivedFieldsInRecordMap) {
        this.groupFieldValue = groupFieldValue;
        this.maxTs = maxTs;
        this.minTs = minTs;
        this.timeStampFieldSimpleName = timeStampFieldSimpleName;
        this.fieldMap = fieldMap;
        this.metaDictionary = metaDictionary;
        this.derivedFieldsInRecordMap = derivedFieldsInRecordMap;
    }

    public Row set(Row t) throws Exception {

        for (String nfFieldName : fieldMap.keySet()) {
            String fieldName = nfFieldName.substring(nfFieldName.indexOf('.') + 1);
            Field fieldMeta = fieldMap.get(nfFieldName);
            //1.1 get the sub-group meta
            // For example, given a field of a.b.c, the base table (a) has a sub-group (b)
            // which is a list of records that have a field of c.
            // To get a list of value for c, it is necessary to get the object of b left,
            // and fetch its each record to extract the value of c to compose the result list.
            String subGroupFieldName = fieldName.substring(0, fieldName.lastIndexOf('.'));
            int pos = subGroupFieldName.lastIndexOf('.');
            String groupName = subGroupFieldName.substring(0, pos);
            String simpleFieldName = subGroupFieldName.substring(pos + 1);

            RecordMetadataInterface subGroupMeta = (RecordMetadataInterface) metaDictionary
                .get(groupName)
                .getFieldMeta(simpleFieldName);

            Field subGroupField = subGroupMeta.getField();
            FieldType subGroupFieldType = subGroupField.getType();

            // 2. find out the field meta for time stamp field
            RecordMetadataInterface tsMeta = (RecordMetadataInterface) metaDictionary
                .get(subGroupFieldName)
                .getFieldMeta(timeStampFieldSimpleName);
            Field tsField = tsMeta.getField();

            if (subGroupFieldType instanceof CollectionFieldType) {
                List<Object> groupValue = (List<Object>) groupFieldValue.getValue(subGroupField);
                if (fieldMeta.getType().getID() == PrimitiveType.STRING.getId()) {
                    List<String> ret = new ArrayList<>();
                    groupValue.forEach((recordObj) -> {
                        Record record = (Record) recordObj;
                        long ts = record.getLong(tsField);
                        if (ts <= maxTs && ts >= minTs) {
                            if (fieldMeta.isDerived()) {
                                if (derivedFieldsInRecordMap.containsKey(fieldMeta)) {
                                    ret.add(derivedFieldsInRecordMap
                                            .get(fieldMeta)
                                            .getValue(fieldMeta, record)
                                            .toString());
                                } else {
                                    ret.add(fieldMeta.getDefaultValue().toString());
                                }
                            } else {
                                ret.add(record.getString(fieldMeta));
                            }
                        }
                    });
                    t.setColumnValue(
                        nfFieldName,
//                        ret.isEmpty() ? ValueUtils.nullValue :
                            new Value(ret, ValueUtils.ListStringDataType));
                } else if (fieldMeta.getType().getID() == PrimitiveType.INT64.getId()
                    || fieldMeta.getType().getID() == PrimitiveType.INT32.getId()
                    || fieldMeta.getType().getID() == PrimitiveType.INT16.getId() ) {
                    List<Long> ret = new ArrayList<>();
                    groupValue.forEach((recordObj) -> {
                        Record record = (Record) recordObj;
                        long ts = record.getLong(tsField);
                        if (ts <= maxTs && ts >= minTs) {
                            Number num = getNumber(fieldMeta, record);
                            ret.add(num.longValue());
                        }
                    });
                    t.setColumnValue(
                        nfFieldName,
//                        ret.isEmpty() ? ValueUtils.nullValue :
                            new Value(ret, ValueUtils.ListLongDataType));
                } else if (fieldMeta.getType().getID() == PrimitiveType.FLOAT.getId()
                    || fieldMeta.getType().getID() == PrimitiveType.DOUBLE.getId() ) {
                    List<Double> ret = new ArrayList<>();
                    groupValue.forEach((recordObj) -> {
                        Record record = (Record) recordObj;
                        long ts = record.getLong(tsField);
                        if (ts <= maxTs && ts >= minTs) {
                            Number num = getNumber(fieldMeta, record);
                            ret.add(num.doubleValue());
                        }
                    });
                    t.setColumnValue(
                        nfFieldName,
//                        ret.isEmpty() ? ValueUtils.nullValue :
                            new Value(ret, ValueUtils.ListFloatDataType));
                }
            } else {
                throw new Exception(
                    String.format("Not support %s", subGroupFieldType));
            }

        }

        return t;
    }

    private Number getNumber(Field fieldMeta, Record record) {
        Number num;
        if (fieldMeta.isDerived()) {
            if (derivedFieldsInRecordMap.containsKey(fieldMeta)) {
                num = (Number) derivedFieldsInRecordMap
                        .get(fieldMeta)
                        .getValue(fieldMeta, record);
            } else {
                num = (Number) fieldMeta.getDefaultValue();
            }
        } else {
            num = (Number) record.getValue(fieldMeta);
        }
        return num;
    }
}
