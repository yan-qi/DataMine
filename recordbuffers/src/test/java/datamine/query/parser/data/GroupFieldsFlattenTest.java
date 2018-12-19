package datamine.query.parser.data;

import datamine.query.data.GroupFieldsFlatten;
import datamine.query.data.Row;
import datamine.query.parser.ProfileQuery;
import datamine.storage.idl.Field;
import datamine.storage.recordbuffers.Record;
import datamine.storage.recordbuffers.RecordBufferMeta;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GroupFieldsFlattenTest {

    public static final Logger LOG = LoggerFactory.getLogger(GroupFieldsFlattenTest.class);

    ProfileQuery profileQuery = new ProfileQuery(
        "datamine.storage.recordbuffers.example.model");

    private Object createRecord() {

        return ExampleDataGenerator.createRecord();

    }

    private GroupFieldsFlatten getFieldsFlattenOperation(String query) throws Exception {
        profileQuery.parse(query);

        Map<String, Field> NFFieldMap =
            profileQuery.getParsingResult().getNestedFuncFieldMap();
        Map<String, RecordBufferMeta> dictionary =
            profileQuery.getParsingResult().getTableColumnMetadataMap();

        Object record = createRecord();

        long minTs = profileQuery.getParsingResult().getStartTimeStamp();
        long maxTs = profileQuery.getParsingResult().getEndTimeStamp();
        return new GroupFieldsFlatten((Record) record, minTs, maxTs, "event_time",
            NFFieldMap, dictionary, null);
    }

    @Test
    public void test1stLevelFields() throws Exception {
        String query = "sel long_required_column col1, nf_count(main_table.nested_table_column.int_required_column) " +
            "from main_table " +
            "when [2012/01/02, 2019/02/03] time_zone EST";
        GroupFieldsFlatten gf = getFieldsFlattenOperation(query);
        Row t = Row.newInstance();
        t = gf.set(t);
        System.out.println(t);
        LOG.info("YQ: new tuple -> (\n\n" + t + ")");
    }


}
