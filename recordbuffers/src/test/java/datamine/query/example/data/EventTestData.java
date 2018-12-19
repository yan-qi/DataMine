package datamine.query.example.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datamine.query.example.interfaces.EventInterface;
import datamine.query.example.model.EventMetadata;
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
public class EventTestData extends AbstractTestData<EventInterface, EventMetadata> {

    public EventTestData(List<EnumMap<EventMetadata, Object>> input) {
        super(input);
    }

    @Override
    public List<EventInterface> createObjects(RecordBuilderInterface builder) {
		List<EventInterface> records = Lists.newArrayList();
		for (EnumMap<EventMetadata, Object> cur : data) {
			EventInterface record = builder.build(EventInterface.class);
			
			if (cur.containsKey(EventMetadata.EVENT_TIME)) {
				record.setEventTime((Long) cur.get(EventMetadata.EVENT_TIME));
			}

			if (cur.containsKey(EventMetadata.EVENT_ID)) {
				record.setEventId((String) cur.get(EventMetadata.EVENT_ID));
			}

			if (cur.containsKey(EventMetadata.DURATION)) {
				record.setDuration((Double) cur.get(EventMetadata.DURATION));
			}

			if (cur.containsKey(EventMetadata.POST_TIME)) {
				record.setPostTime((Long) cur.get(EventMetadata.POST_TIME));
			}

			if (cur.containsKey(EventMetadata.URL)) {
				record.setUrl((String) cur.get(EventMetadata.URL));
			}

			if (cur.containsKey(EventMetadata.START)) {
				record.setStart((Double) cur.get(EventMetadata.START));
			}

			if (cur.containsKey(EventMetadata.BASIC_ATTRS)) {
				record.setBasicAttrs(new BasicTestData((List) cur.get(EventMetadata.BASIC_ATTRS)).createObjects(builder).get(0));
			}

			if (cur.containsKey(EventMetadata.ATTR_LIST)) {
				record.setAttrList(new BasicTestData((List) cur.get(EventMetadata.ATTR_LIST)).createObjects(builder));
			}

			records.add(record);
		}
		return records;
    }

    @Override
    public void assertObjects(List<EventInterface> objectList) {
		int size = objectList.size();
		Assert.assertEquals(size, data.size());
		for (int i = 0; i < size; ++i) {
			
			if (data.get(i).containsKey(EventMetadata.EVENT_TIME)) {
				Assert.assertEquals(objectList.get(i).getEventTime(), data.get(i).get(EventMetadata.EVENT_TIME));
			}

			if (data.get(i).containsKey(EventMetadata.EVENT_ID)) {
				Assert.assertEquals(objectList.get(i).getEventId(), data.get(i).get(EventMetadata.EVENT_ID));
			}

			if (data.get(i).containsKey(EventMetadata.DURATION)) {
				Assert.assertEquals(objectList.get(i).getDuration(), data.get(i).get(EventMetadata.DURATION));
			}

			if (data.get(i).containsKey(EventMetadata.POST_TIME)) {
				Assert.assertEquals(objectList.get(i).getPostTime(), data.get(i).get(EventMetadata.POST_TIME));
			}

			if (data.get(i).containsKey(EventMetadata.URL)) {
				Assert.assertEquals(objectList.get(i).getUrl(), data.get(i).get(EventMetadata.URL));
			}

			if (data.get(i).containsKey(EventMetadata.START)) {
				Assert.assertEquals(objectList.get(i).getStart(), data.get(i).get(EventMetadata.START));
			}

			if (data.get(i).containsKey(EventMetadata.BASIC_ATTRS)) {
				new BasicTestData((List) data.get(i).get(EventMetadata.BASIC_ATTRS)).assertObjects(Lists.newArrayList(objectList.get(i).getBasicAttrs()));
			}

			if (data.get(i).containsKey(EventMetadata.ATTR_LIST)) {
				new BasicTestData((List) data.get(i).get(EventMetadata.ATTR_LIST)).assertObjects(objectList.get(i).getAttrList());
			}

		}
	}

    public static List<EnumMap<EventMetadata, Object>> createInputData(int num) {
		List<EnumMap<EventMetadata, Object>> dataList = Lists.newArrayList();
		for (int i = 0; i < num; ++i) {
			EnumMap<EventMetadata, Object> dataMap = Maps.newEnumMap(EventMetadata.class);
			
			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.EVENT_TIME.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.EVENT_TIME, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.EVENT_ID.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.EVENT_ID, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.DURATION.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.DURATION, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.POST_TIME.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.POST_TIME, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.URL.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.URL, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)EventMetadata.START.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(EventMetadata.START, val);
				}
			}

			{
				Object val = BasicTestData.createInputData(1);
				if (val != null && !((List) val).isEmpty()) {
					dataMap.put(EventMetadata.BASIC_ATTRS, val);
				}
			}

			{
				Object val = BasicTestData.createInputData(3);
				if (val != null && !((List) val).isEmpty()) {
					dataMap.put(EventMetadata.ATTR_LIST, val);
				}
			}

			if (!dataMap.isEmpty()) {
				dataList.add(dataMap);
			}
		}
		return dataList;
	}

}


