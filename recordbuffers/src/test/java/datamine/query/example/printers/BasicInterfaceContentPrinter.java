package datamine.query.example.printers;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.BasicInterface;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class BasicInterfaceContentPrinter implements UnaryOperatorInterface<BasicInterface, String> {

	@Override
	public String apply(BasicInterface input) {
		StringBuffer out = new StringBuffer();
		out.append("{\n");
		out.append("event_time = ").append(input.getEventTime()).append("\n");

		out.append("}");
		return out.toString();
	}

}

