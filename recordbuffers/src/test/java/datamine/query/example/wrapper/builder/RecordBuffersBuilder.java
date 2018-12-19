package datamine.query.example.wrapper.builder;


import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.interfaces.ProfileInterface;
import datamine.query.example.wrapper.BasicRecord;
import datamine.query.example.wrapper.EventRecord;
import datamine.query.example.wrapper.ProfileRecord;
import datamine.storage.api.BaseInterface;
import datamine.storage.api.RecordBuilderInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class RecordBuffersBuilder implements RecordBuilderInterface {

	static final Logger LOG = LoggerFactory.getLogger(RecordBuffersBuilder.class);

	@SuppressWarnings("unchecked")
	@Override
	public <T extends BaseInterface> T build(Class<T> tableClass) {

		try {
			
		if (tableClass == EventInterface.class) {
			return (T) EventRecord.class.newInstance();
		}
		else
		if (tableClass == BasicInterface.class) {
			return (T) BasicRecord.class.newInstance();
		}
		else
		if (tableClass == ProfileInterface.class) {
			return (T) ProfileRecord.class.newInstance();
		}

		} catch (InstantiationException e) {
			LOG.error("The object can not be created for " + tableClass + ":" + e);
		} catch (IllegalAccessException e) {
			LOG.error("The object can not be created for " + tableClass + ":" + e);
		}

		LOG.error("Cannot create an instance for "+tableClass);
		throw new IllegalArgumentException("Not support for the record of "+tableClass);
	}
}


