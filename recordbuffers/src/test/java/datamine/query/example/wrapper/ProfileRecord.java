package datamine.query.example.wrapper;

import com.google.common.collect.Lists;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.interfaces.ProfileInterface;
import datamine.query.example.model.EventMetadata;
import datamine.query.example.model.ProfileMetadata;
import datamine.storage.api.BaseInterface;
import datamine.storage.recordbuffers.Record;
import datamine.storage.recordbuffers.RecordBuffer;
import datamine.storage.recordbuffers.WritableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class ProfileRecord implements ProfileInterface {
    static final Logger LOG = LoggerFactory.getLogger(ProfileRecord.class);

    Record<ProfileMetadata> value = null;
	

    public ProfileRecord() {
        value = new WritableRecord<ProfileMetadata>(ProfileMetadata.class);
    }

    public ProfileRecord(Record<ProfileMetadata> value) {
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
            this.value = (Record<ProfileMetadata>) obj;
        }else{
            throw new IllegalArgumentException("Not Support type of "+obj.getClass());
        }
    }

    @Override
    public void referTo(BaseInterface right) {
        this.value = (Record<ProfileMetadata>) right.getBaseObject();
    }

    @Override
    public void copyFrom(BaseInterface right) {
		// note that it must be deep copy!!
		this.value = new WritableRecord<ProfileMetadata>(ProfileMetadata.class,
			new RecordBuffer(((Record) right.getBaseObject()).getRecordBuffer()));
    }

    @Override
    public boolean equals(Object that) {
        if (that == null){
            return false;
        }
        if (that instanceof ProfileRecord){
            return this.getBaseObject().equals(((ProfileRecord) that).getBaseObject());
        }
        return false;
    }

    @Override
    public String getProfileId() {
        
        return this.value.getString(ProfileMetadata.PROFILE_ID);
    }

    @Override
    public long getCreationTime() {
        
        return this.value.getLong(ProfileMetadata.CREATION_TIME);
    }

    @Override
    public long getLastModifiedTime() {
        
        return this.value.getLong(ProfileMetadata.LAST_MODIFIED_TIME);
    }

    @Override
    public List<EventInterface> getEvents() {
            	List<EventInterface> dList = Lists.newArrayList();
		List<Object> sList = (List<Object>) this.value.getValue(ProfileMetadata.EVENTS);
		if(sList != null) {
		   	for (Object cur : sList) {
    			dList.add(new EventRecord((Record)cur));
	    	}
		}
       return dList;

        
    }



	@Override
	public void setProfileId(String input) {
		if (1 == 1) {
			
			this.value.setValue(ProfileMetadata.PROFILE_ID, input);
		}
	}


	@Override
	public void setCreationTime(long input) {
		if (1 == 1) {
			
			this.value.setValue(ProfileMetadata.CREATION_TIME, input);
		}
	}


	@Override
	public void setLastModifiedTime(long input) {
		if (1 == 1) {
			
			this.value.setValue(ProfileMetadata.LAST_MODIFIED_TIME, input);
		}
	}


	@Override
	public void setEvents(List<EventInterface> input) {
		if (input != null && !input.isEmpty()) {
			
		List<Record> list = Lists.newArrayList();
		for(EventInterface elem : input){
			EventRecord iRec = (EventRecord) elem;
			Record<EventMetadata> rec = (Record<EventMetadata>) iRec.getBaseObject();
			list.add(rec);
		}
		this.value.setValue(ProfileMetadata.EVENTS, list);

			
		}
	}


    @Override
    public String getProfileIdDefaultValue() {
        throw new NullPointerException("Require a valid value for profile_id! Make sure the column has been selected!");
    }

    @Override
    public long getCreationTimeDefaultValue() {
        throw new NullPointerException("Require a valid value for creation_time! Make sure the column has been selected!");
    }

    @Override
    public long getLastModifiedTimeDefaultValue() {
        throw new NullPointerException("Require a valid value for last_modified_time! Make sure the column has been selected!");
    }


	@Override
	public int getEventsSize() {
		return this.value.getListSize(ProfileMetadata.EVENTS);
	}




}

