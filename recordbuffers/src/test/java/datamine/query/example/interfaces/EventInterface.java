package datamine.query.example.interfaces;

import datamine.storage.api.BaseInterface;

import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public interface EventInterface extends BaseInterface, Comparable<EventInterface> {

		public long getEventTime();
		public String getEventId();
		public double getDuration();
		public long getPostTime();
		public String getUrl();
		public double getStart();
		public BasicInterface getBasicAttrs();
		public List<BasicInterface> getAttrList();

		public void setEventTime(long input);
		public void setEventId(String input);
		public void setDuration(double input);
		public void setPostTime(long input);
		public void setUrl(String input);
		public void setStart(double input);
		public void setBasicAttrs(BasicInterface input);
		public void setAttrList(List<BasicInterface> input);

		public long getEventTimeDefaultValue();
		public String getEventIdDefaultValue();
		public double getDurationDefaultValue();
		public long getPostTimeDefaultValue();
		public String getUrlDefaultValue();
		public double getStartDefaultValue();

		public int getAttrListSize();


}

