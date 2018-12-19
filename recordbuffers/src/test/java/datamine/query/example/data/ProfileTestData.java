package datamine.query.example.data;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import datamine.query.example.interfaces.ProfileInterface;
import datamine.query.example.model.ProfileMetadata;
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
public class ProfileTestData extends AbstractTestData<ProfileInterface, ProfileMetadata> {

    public ProfileTestData(List<EnumMap<ProfileMetadata, Object>> input) {
        super(input);
    }

    @Override
    public List<ProfileInterface> createObjects(RecordBuilderInterface builder) {
		List<ProfileInterface> records = Lists.newArrayList();
		for (EnumMap<ProfileMetadata, Object> cur : data) {
			ProfileInterface record = builder.build(ProfileInterface.class);
			
			if (cur.containsKey(ProfileMetadata.PROFILE_ID)) {
				record.setProfileId((String) cur.get(ProfileMetadata.PROFILE_ID));
			}

			if (cur.containsKey(ProfileMetadata.CREATION_TIME)) {
				record.setCreationTime((Long) cur.get(ProfileMetadata.CREATION_TIME));
			}

			if (cur.containsKey(ProfileMetadata.LAST_MODIFIED_TIME)) {
				record.setLastModifiedTime((Long) cur.get(ProfileMetadata.LAST_MODIFIED_TIME));
			}

			if (cur.containsKey(ProfileMetadata.EVENTS)) {
				record.setEvents(new EventTestData((List) cur.get(ProfileMetadata.EVENTS)).createObjects(builder));
			}

			records.add(record);
		}
		return records;
    }

    @Override
    public void assertObjects(List<ProfileInterface> objectList) {
		int size = objectList.size();
		Assert.assertEquals(size, data.size());
		for (int i = 0; i < size; ++i) {
			
			if (data.get(i).containsKey(ProfileMetadata.PROFILE_ID)) {
				Assert.assertEquals(objectList.get(i).getProfileId(), data.get(i).get(ProfileMetadata.PROFILE_ID));
			}

			if (data.get(i).containsKey(ProfileMetadata.CREATION_TIME)) {
				Assert.assertEquals(objectList.get(i).getCreationTime(), data.get(i).get(ProfileMetadata.CREATION_TIME));
			}

			if (data.get(i).containsKey(ProfileMetadata.LAST_MODIFIED_TIME)) {
				Assert.assertEquals(objectList.get(i).getLastModifiedTime(), data.get(i).get(ProfileMetadata.LAST_MODIFIED_TIME));
			}

			if (data.get(i).containsKey(ProfileMetadata.EVENTS)) {
				new EventTestData((List) data.get(i).get(ProfileMetadata.EVENTS)).assertObjects(objectList.get(i).getEvents());
			}

		}
	}

    public static List<EnumMap<ProfileMetadata, Object>> createInputData(int num) {
		List<EnumMap<ProfileMetadata, Object>> dataList = Lists.newArrayList();
		for (int i = 0; i < num; ++i) {
			EnumMap<ProfileMetadata, Object> dataMap = Maps.newEnumMap(ProfileMetadata.class);
			
			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)ProfileMetadata.PROFILE_ID.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(ProfileMetadata.PROFILE_ID, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)ProfileMetadata.CREATION_TIME.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(ProfileMetadata.CREATION_TIME, val);
				}
			}

			{
				Object val = RandomValueGenerator.getValueOf(((PrimitiveFieldType)ProfileMetadata.LAST_MODIFIED_TIME.getField().getType()).getPrimitiveType());
				if (val != null) {
					dataMap.put(ProfileMetadata.LAST_MODIFIED_TIME, val);
				}
			}

			{
				Object val = EventTestData.createInputData(3);
				if (val != null && !((List) val).isEmpty()) {
					dataMap.put(ProfileMetadata.EVENTS, val);
				}
			}

			if (!dataMap.isEmpty()) {
				dataList.add(dataMap);
			}
		}
		return dataList;
	}

}


