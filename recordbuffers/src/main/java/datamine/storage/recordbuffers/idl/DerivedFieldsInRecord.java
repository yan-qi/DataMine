package datamine.storage.recordbuffers.idl;

import datamine.storage.api.RecordMetadataInterface;
import datamine.storage.idl.Field;
import datamine.storage.recordbuffers.Record;

public interface DerivedFieldsInRecord {
    <T extends Enum<T> & RecordMetadataInterface> Object getValue(Field field, Record<T> record);
}
