package datamine.query.example.convertors;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.storage.api.RecordBuilderInterface;

import java.util.ArrayList;
import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class EventInterfaceConvertor implements UnaryOperatorInterface<EventInterface, EventInterface> {

	private RecordBuilderInterface builder;

   public EventInterfaceConvertor(RecordBuilderInterface builder) {
   	this.builder = builder;
	}
	@Override
	public EventInterface apply(EventInterface input) {
		EventInterface output = builder.build(EventInterface.class);
		output.setEventTime(input.getEventTime());

		output.setEventId(input.getEventId());

		output.setDuration(input.getDuration());

		output.setPostTime(input.getPostTime());

		output.setUrl(input.getUrl());

		output.setStart(input.getStart());

		{
			BasicInterfaceConvertor convertor = new BasicInterfaceConvertor(builder);
			if (input.getBasicAttrs() != null) {
       		output.setBasicAttrs(convertor.apply(input.getBasicAttrs()));
			}
		}
		{
			List<BasicInterface> list = new ArrayList<BasicInterface>();
			BasicInterfaceConvertor convertor = new BasicInterfaceConvertor(builder);
			for (BasicInterface tuple: input.getAttrList()) {
				list.add(convertor.apply(tuple));
			}
			output.setAttrList(list);
		}

		return output;
	}

}

