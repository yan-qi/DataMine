package datamine.query.example.wrapper;

import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.model.BasicMetadata;
import datamine.storage.api.BaseInterface;
import datamine.storage.recordbuffers.Record;
import datamine.storage.recordbuffers.RecordBuffer;
import datamine.storage.recordbuffers.WritableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class BasicRecord implements BasicInterface {
    static final Logger LOG = LoggerFactory.getLogger(BasicRecord.class);

    Record<BasicMetadata> value = null;
	

    public BasicRecord() {
        value = new WritableRecord<BasicMetadata>(BasicMetadata.class);
    }

    public BasicRecord(Record<BasicMetadata> value) {
        this.value = value;
    }

    @Override
    public Object getBaseObject() {
        return value;
    }

    @Override
    public Class getBaseClass() {
        return Record.class;
    }

    @Override
    public void setBaseObject(Object obj) {
        if (obj instanceof Record){
            this.value = (Record<BasicMetadata>) obj;
        }else{
            throw new IllegalArgumentException("Not Support type of "+obj.getClass());
        }
    }

    @Override
    public void referTo(BaseInterface right) {
        this.value = (Record<BasicMetadata>) right.getBaseObject();
    }

    @Override
    public void copyFrom(BaseInterface right) {
		// note that it must be deep copy!!
		this.value = new WritableRecord<BasicMetadata>(BasicMetadata.class,
			new RecordBuffer(((Record) right.getBaseObject()).getRecordBuffer()));
    }

    @Override
    public boolean equals(Object that) {
        if (that == null){
            return false;
        }
        if (that instanceof BasicRecord){
            return this.getBaseObject().equals(((BasicRecord) that).getBaseObject());
        }
        return false;
    }

    @Override
    public long getEventTime() {
        
        return this.value.getLong(BasicMetadata.EVENT_TIME);
    }



	@Override
	public void setEventTime(long input) {
		if (1 == 1) {
			
			this.value.setValue(BasicMetadata.EVENT_TIME, input);
		}
	}


    @Override
    public long getEventTimeDefaultValue() {
        throw new NullPointerException("Require a valid value for event_time! Make sure the column has been selected!");
    }





}

