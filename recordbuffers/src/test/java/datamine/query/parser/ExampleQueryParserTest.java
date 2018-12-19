package datamine.query.parser;

import org.junit.Test;

public class ExampleQueryParserTest {

    ProfileQuery profileQuery = new ProfileQuery(
        "datamine.storage.recordbuffers.example.model");

    @Test (expected = IllegalArgumentException.class)
    public void parsingTableList1() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table, main_table1 dates [2012/01/02, 2013/02/03] time_zone EST";
//        query = "sel GetDate(c3, UTC, \"yyyy_MM_dd\"), c1 as col1, c2 col2 from t1 dates [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
    }

//    @Test (expected = IllegalArgumentException.class)
    public void parsingTableList2() {
        String query = "sel long_required_column col1, main_table.int_sorted_column from main_table dates [2012/01/02, 2013/02/03] time_zone EST";
//        query = "sel GetDate(c3, UTC, \"yyyy_MM_dd\"), c1 as col1, c2 col2 from t1 dates [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
    }

    @Test
    public void parsingTableList3() {
        String query = "sel getDate(long_required_column) col1, main_table.int_sorted_column, count(*) from main_table dates [2012/01/02, 2013/02/03] time_zone EST";
//        query = "sel GetDate(c3, UTC, \"yyyy_MM_dd\"), c1 as col1, c2 col2 from t1 dates [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
    }

    @Test
    public void parsingTableList4() {
        String query = "sel getDate(long_required_column) col1, main_table.int_sorted_column, count(distinct int_sorted_column) from main_table dates [2012/01/02, 2013/02/03] time_zone EST";
//        query = "sel GetDate(c3, UTC, \"yyyy_MM_dd\"), c1 as col1, c2 col2 from t1 dates [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
    }

    @Test
    public void parsingTableDerivedFiled() {
        String query = "sel string_derived_column, count(distinct int_sorted_column) from main_table dates [2012/01/02, 2013/02/03] time_zone EST";
//        query = "sel GetDate(c3, UTC, \"yyyy_MM_dd\"), c1 as col1, c2 col2 from t1 dates [2012/01/02, 2013/02/03] time_zone EST";
        profileQuery.parse(query);
    }
}


