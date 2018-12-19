package datamine.query.example.convertors;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.interfaces.ProfileInterface;
import datamine.storage.api.RecordBuilderInterface;

import java.util.ArrayList;
import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class ProfileInterfaceConvertor implements UnaryOperatorInterface<ProfileInterface, ProfileInterface> {

	private RecordBuilderInterface builder;

   public ProfileInterfaceConvertor(RecordBuilderInterface builder) {
   	this.builder = builder;
	}
	@Override
	public ProfileInterface apply(ProfileInterface input) {
		ProfileInterface output = builder.build(ProfileInterface.class);
		output.setProfileId(input.getProfileId());

		output.setCreationTime(input.getCreationTime());

		output.setLastModifiedTime(input.getLastModifiedTime());

		{
			List<EventInterface> list = new ArrayList<EventInterface>();
			EventInterfaceConvertor convertor = new EventInterfaceConvertor(builder);
			for (EventInterface tuple: input.getEvents()) {
				list.add(convertor.apply(tuple));
			}
			output.setEvents(list);
		}

		return output;
	}

}

