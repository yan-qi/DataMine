package datamine.query.example.convertors;

import datamine.operator.UnaryOperatorInterface;
import datamine.query.example.interfaces.BasicInterface;
import datamine.storage.api.RecordBuilderInterface;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class BasicInterfaceConvertor implements UnaryOperatorInterface<BasicInterface, BasicInterface> {

	private RecordBuilderInterface builder;

   public BasicInterfaceConvertor(RecordBuilderInterface builder) {
   	this.builder = builder;
	}
	@Override
	public BasicInterface apply(BasicInterface input) {
		BasicInterface output = builder.build(BasicInterface.class);
		output.setEventTime(input.getEventTime());


		return output;
	}

}

