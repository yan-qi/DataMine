package datamine.query.parser.data;

import datamine.query.data.GroupFlatten;
import datamine.query.data.Row;
import datamine.query.parser.ProfileQuery;
import datamine.storage.idl.Field;
import datamine.storage.recordbuffers.RecordBufferMeta;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class GroupFlattenTest {

    public static final Logger LOG = LoggerFactory.getLogger(GroupFlattenTest.class);

    ProfileQuery profileQuery = new ProfileQuery(
        "datamine.storage.recordbuffers.example.model");

    private Object createRecord() {

        return ExampleDataGenerator.createRecord();

    }

    private GroupFlatten getFlattenOperation(String query) throws Exception {
        profileQuery.parse(query);

        Map<String, Field> baseFieldMap =
            profileQuery.getParsingResult().getBaseFieldMap();
        Map<String, RecordBufferMeta> dictionary =
            profileQuery.getParsingResult().getTableColumnMetadataMap();
        Set<String> whereTables =
            profileQuery.getParsingResult().getWhereTables();

        Object record = createRecord();

        return new GroupFlatten(record, "main_table",
            whereTables, baseFieldMap, dictionary, null);
    }

    @Test
    public void test1stLevelFields() throws Exception {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table when [2012/01/02, 2013/02/03] time_zone EST";
        GroupFlatten gf = getFlattenOperation(query);
        Row t = Row.newInstance();
        int size = 0;
        while (gf.hasNext()) {
            size++;
            t = gf.next(t);
            LOG.debug("YQ: new tuple -> (\n\n" + t + ")");
        }
        assertEquals(size, 1);
    }

    //@Test
    public void test2ndLevelFields() throws Exception {
        String query =
            "sel long_required_column col1, nested_table_column.int_required_column " +
                "from main_table, main_table.nested_table_column " +
                "when [2012/01/02, 2013/02/03] time_zone EST";

        GroupFlatten gf = getFlattenOperation(query);
        Row t = Row.newInstance();
        int size = 0;
        while (gf.hasNext()) {
            size++;
            t = gf.next(t);
            LOG.debug("YQ: new tuple -> (\n\n" + t + ")");
        }
        assertEquals(size, 1);
    }

    //@Test
    public void test3rdLevelFields1() throws Exception {
        String query =
            "sel long_required_column col1, nested_table_column.int_required_column, nested_table_column.nested_table_column.byte_required_column " +
                "from main_table, main_table.nested_table_column, main_table.nested_table_column.nested_table_column " +
                "when [2012/01/02, 2013/02/03] time_zone EST";

        GroupFlatten gf = getFlattenOperation(query);
        Row t = Row.newInstance();
        int size = 0;
        while (gf.hasNext()) {
            size++;
            t = gf.next(t);
            LOG.debug("YQ: new tuple -> (\n\n" + t + ")");
        }
        assertEquals(2, size);
    }

    //@Test
    public void test3rdLevelFields2() throws Exception {
        String query =
            "sel long_required_column col1, nested_table_column.nested_table_column.byte_required_column " +
                "from main_table, main_table.nested_table_column, main_table.nested_table_column.nested_table_column " +
                "when [2012/01/02, 2013/02/03] time_zone EST";

        GroupFlatten gf = getFlattenOperation(query);
        Row t = Row.newInstance();
        int size = 0;
        while (gf.hasNext()) {
            size++;
            t = gf.next(t);
            LOG.debug("YQ: new tuple -> (\n\n" + t + ")");
        }
        assertEquals(2, size);
    }


    @Test
    public void test1stLevelStructFields() throws Exception {
        String query =
            "sel struct_column.int_sorted_column " +
                "from main_table " +
                "when [2012/01/02, 2013/02/03] time_zone EST";
//            "sel long_required_column col1, nested_table_column.int_required_column, struct_column.int_sorted_column " +
//                "from main_table, main_table.nested_table_column " +
//                "when [2012/01/02, 2013/02/03] time_zone EST";

        GroupFlatten gf = getFlattenOperation(query);
        Row t = Row.newInstance();
        int size = 0;
        while (gf.hasNext()) {
            size++;
            t = gf.next(t);
            LOG.debug("YQ: new tuple -> \n(\n" + t + ")");
        }
        assertEquals(1, size);
    }

//    @Test
    public void test() {
        String file = "/group/amp_metrics/aggregates/lookup-data/blackListedBuilds.txt";
        System.out.print(file.indexOf('/'));
        System.out.print(file.indexOf('/') < 0 ? file : file.substring(1+file.lastIndexOf('/')));
    }
}
