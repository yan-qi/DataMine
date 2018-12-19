package datamine.query.example.interfaces;

import datamine.storage.api.BaseInterface;

import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public interface ProfileInterface extends BaseInterface {

		public String getProfileId();
		public long getCreationTime();
		public long getLastModifiedTime();
		public List<EventInterface> getEvents();

		public void setProfileId(String input);
		public void setCreationTime(long input);
		public void setLastModifiedTime(long input);
		public void setEvents(List<EventInterface> input);

		public String getProfileIdDefaultValue();
		public long getCreationTimeDefaultValue();
		public long getLastModifiedTimeDefaultValue();

		public int getEventsSize();


}

