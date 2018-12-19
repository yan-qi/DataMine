package datamine.query.example.printers;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.interfaces.EventInterface;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class EventInterfaceContentPrinter implements UnaryOperatorInterface<EventInterface, String> {

	@Override
	public String apply(EventInterface input) {
		StringBuffer out = new StringBuffer();
		out.append("{\n");
		out.append("event_time = ").append(input.getEventTime()).append("\n");
		out.append("event_id = ").append(input.getEventId()).append("\n");
		out.append("duration = ").append(input.getDuration()).append("\n");
		out.append("post_time = ").append(input.getPostTime()).append("\n");
		out.append("url = ").append(input.getUrl()).append("\n");
		out.append("start = ").append(input.getStart()).append("\n");
		{
			BasicInterfaceContentPrinter printer = new BasicInterfaceContentPrinter();
			if (input.getBasicAttrs() != null) {
       		out.append("basic_attrs = ").append(printer.apply(input.getBasicAttrs())).append("\n");
			}
		}
		{
			BasicInterfaceContentPrinter printer = new BasicInterfaceContentPrinter();
			out.append("attr_list = ").append("[").append("\n");
			for (BasicInterface tuple: input.getAttrList()) {
				out.append(printer.apply(tuple)).append("\n");
			}
			out.append("]").append("\n");
		}

		out.append("}");
		return out.toString();
	}

}

