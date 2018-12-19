package datamine.query.example.wrapper;

import com.google.common.collect.Lists;
import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.model.BasicMetadata;
import datamine.query.example.model.EventMetadata;
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
public class EventRecord implements EventInterface {
    static final Logger LOG = LoggerFactory.getLogger(EventRecord.class);

    Record<EventMetadata> value = null;
	

    public EventRecord() {
        value = new WritableRecord<EventMetadata>(EventMetadata.class);
    }

    public EventRecord(Record<EventMetadata> value) {
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
            this.value = (Record<EventMetadata>) obj;
        }else{
            throw new IllegalArgumentException("Not Support type of "+obj.getClass());
        }
    }

    @Override
    public void referTo(BaseInterface right) {
        this.value = (Record<EventMetadata>) right.getBaseObject();
    }

    @Override
    public void copyFrom(BaseInterface right) {
		// note that it must be deep copy!!
		this.value = new WritableRecord<EventMetadata>(EventMetadata.class,
			new RecordBuffer(((Record) right.getBaseObject()).getRecordBuffer()));
    }

    @Override
    public boolean equals(Object that) {
        if (that == null){
            return false;
        }
        if (that instanceof EventRecord){
            return this.getBaseObject().equals(((EventRecord) that).getBaseObject());
        }
        return false;
    }

    @Override
    public long getEventTime() {
        
        return this.value.getLong(EventMetadata.EVENT_TIME);
    }

    @Override
    public String getEventId() {
        
        return this.value.getString(EventMetadata.EVENT_ID);
    }

    @Override
    public double getDuration() {
        
        return this.value.getDouble(EventMetadata.DURATION);
    }

    @Override
    public long getPostTime() {
        
        return this.value.getLong(EventMetadata.POST_TIME);
    }

    @Override
    public String getUrl() {
        
        return this.value.getString(EventMetadata.URL);
    }

    @Override
    public double getStart() {
        
        return this.value.getDouble(EventMetadata.START);
    }

    @Override
    public BasicInterface getBasicAttrs() {
        
        
		Record record = (Record) this.value.getValue(EventMetadata.BASIC_ATTRS);
		if (record == null) {
			return null;
		} else {
			return new BasicRecord(record);
		}

    }

    @Override
    public List<BasicInterface> getAttrList() {
            	List<BasicInterface> dList = Lists.newArrayList();
		List<Object> sList = (List<Object>) this.value.getValue(EventMetadata.ATTR_LIST);
		if(sList != null) {
		   	for (Object cur : sList) {
    			dList.add(new BasicRecord((Record)cur));
	    	}
		}
       return dList;

        
    }



	@Override
	public void setEventTime(long input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.EVENT_TIME, input);
		}
	}


	@Override
	public void setEventId(String input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.EVENT_ID, input);
		}
	}


	@Override
	public void setDuration(double input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.DURATION, input);
		}
	}


	@Override
	public void setPostTime(long input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.POST_TIME, input);
		}
	}


	@Override
	public void setUrl(String input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.URL, input);
		}
	}


	@Override
	public void setStart(double input) {
		if (1 == 1) {
			
			this.value.setValue(EventMetadata.START, input);
		}
	}


	@Override
	public void setBasicAttrs(BasicInterface input) {
		if (input != null) {
			
		BasicRecord iRec = (BasicRecord) input;
		Record<BasicMetadata> rec = (Record<BasicMetadata>) iRec.getBaseObject();
		this.value.setValue(EventMetadata.BASIC_ATTRS, rec);

			
		}
	}


	@Override
	public void setAttrList(List<BasicInterface> input) {
		if (input != null && !input.isEmpty()) {
			
		List<Record> list = Lists.newArrayList();
		for(BasicInterface elem : input){
			BasicRecord iRec = (BasicRecord) elem;
			Record<BasicMetadata> rec = (Record<BasicMetadata>) iRec.getBaseObject();
			list.add(rec);
		}
		this.value.setValue(EventMetadata.ATTR_LIST, list);

			
		}
	}


    @Override
    public long getEventTimeDefaultValue() {
        throw new NullPointerException("Require a valid value for event_time! Make sure the column has been selected!");
    }

    @Override
    public String getEventIdDefaultValue() {
        throw new NullPointerException("Require a valid value for event_id! Make sure the column has been selected!");
    }

    @Override
    public double getDurationDefaultValue() {
        throw new NullPointerException("Require a valid value for duration! Make sure the column has been selected!");
    }

    @Override
    public long getPostTimeDefaultValue() {
        throw new NullPointerException("Require a valid value for post_time! Make sure the column has been selected!");
    }

    @Override
    public String getUrlDefaultValue() {
        throw new NullPointerException("Require a valid value for url! Make sure the column has been selected!");
    }

    @Override
    public double getStartDefaultValue() {
        throw new NullPointerException("Require a valid value for start! Make sure the column has been selected!");
    }


	@Override
	public int getAttrListSize() {
		return this.value.getListSize(EventMetadata.ATTR_LIST);
	}


	@Override
	public int compareTo(EventInterface o) {
		if (this == o) return 0;
		if (this == null) return -1;
		if (o == null) return 1;

		if (this.getEventTime() < o.getEventTime()) return -1;
		else if (this.getEventTime() > o.getEventTime()) return 1;
		else return 0;
	}


}

