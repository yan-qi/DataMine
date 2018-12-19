package datamine.query.example.printers;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.interfaces.ProfileInterface;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class ProfileInterfaceContentPrinter implements UnaryOperatorInterface<ProfileInterface, String> {

	@Override
	public String apply(ProfileInterface input) {
		StringBuffer out = new StringBuffer();
		out.append("{\n");
		out.append("profile_id = ").append(input.getProfileId()).append("\n");
		out.append("creation_time = ").append(input.getCreationTime()).append("\n");
		out.append("last_modified_time = ").append(input.getLastModifiedTime()).append("\n");
		{
			EventInterfaceContentPrinter printer = new EventInterfaceContentPrinter();
			out.append("events = ").append("[").append("\n");
			for (EventInterface tuple: input.getEvents()) {
				out.append(printer.apply(tuple)).append("\n");
			}
			out.append("]").append("\n");
		}

		out.append("}");
		return out.toString();
	}

}

