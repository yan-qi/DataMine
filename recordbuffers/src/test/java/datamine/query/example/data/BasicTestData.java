package datamine.query.example.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datamine.query.example.interfaces.BasicInterface;
import datamine.query.example.model.BasicMetadata;
import datamine.storage.api.RecordBuilderInterface;
import datamine.storage.idl.generator.AbstractTestData;
import datamine.storage.idl.generator.RandomValueGenerator;
import datamine.storage.idl.type.PrimitiveFieldType;
import org.testng.Assert;

import java.util.EnumMap;
import java.util.List;


/**
 * DO Not CHANGE! Auto-generated code
 */
public class BasicTestData extends AbstractTestData<BasicInterface, BasicMetadata> {

    public BasicTestData(List<EnumMap<BasicMetadata, Object>> input) {
        super(input);
    }

    @Override
    public List<BasicInterface> createObjects(RecordBuilderInterface builder) {
		List<BasicInterface> records = Lists.newArrayList();
		for (EnumMap<BasicMetadata, Object> cur : data) {
			BasicInterface record = builder.build(BasicInterface.class);
			
			if (cur.containsKey(BasicMetadata.EVENT_TIME)) {
				record.setEventTime((Long) cur.get(BasicMetadata.EVENT_TIME));
			}

			records.add(record);
		}
		return records;
    }

    @Override
    public void assertObjects(List<BasicInterface> objectList) {
		int size = objectList.size();
		Assert.assertEquals(size, data.size());
		for (int i = 0; i < size; ++i) {
			
			if (data.get(i).containsKey(BasicMetadata.EVENT_TIME)) {
				Assert.assertEquals(objectList.get(i).getEventTime(), data.get(i).get(BasicMetadata.EVENT_TIME));
			}

		}
	}

    public static List<EnumMap<BasicMetadata, Object>> createInputData(int num) {
		List<EnumMap<BasicMetadata, Object>> dataList = Lists.newArrayList();
		for (int i = 0; i < num; ++i) {
			EnumMap<BasicMetadata, Object> dataMap = Maps.newEnumMap(BasicMetadata.class);
			
			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType) BasicMetadata.EVENT_TIME.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(BasicMetadata.EVENT_TIME, val);
				}
			}

			if (!dataMap.isEmpty()) {
				dataList.add(dataMap);
			}
		}
		return dataList;
	}

}


