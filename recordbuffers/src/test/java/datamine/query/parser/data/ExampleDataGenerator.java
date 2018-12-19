package datamine.query.parser.data;

import datamine.storage.api.RecordBuilderInterface;
import datamine.storage.recordbuffers.example.interfaces.FirstLevelNestedTableInterface;
import datamine.storage.recordbuffers.example.interfaces.MainTableInterface;
import datamine.storage.recordbuffers.example.interfaces.SecondLevelNestedTableInterface;
import datamine.storage.recordbuffers.example.interfaces.StructTableInterface;
import datamine.storage.recordbuffers.example.wrapper.builder.RecordBuffersBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ExampleDataGenerator {

    private ExampleDataGenerator(){}

    public static Object createRecord() {

        RecordBuilderInterface builder = new RecordBuffersBuilder();
        MainTableInterface mainTable = builder.build(MainTableInterface.class);

        //1. set field value at the top level: profile attributes
        mainTable.setLongRequiredColumn(123l);
        mainTable.setIntSortedColumn(234);
        mainTable.setDoubleColumn(1.23);

        //2. create a nested table at the 2nd level
        List<SecondLevelNestedTableInterface> secondLevelNestedTableInterfaceList = new ArrayList<>();

        //2.1 the 1st record in the nested table (2nd level)
        SecondLevelNestedTableInterface secondLevelNestedTable1 = builder.build(SecondLevelNestedTableInterface.class);
        secondLevelNestedTable1.setByteRequiredColumn((byte)7);
        secondLevelNestedTableInterfaceList.add(secondLevelNestedTable1);

        //2.2 the 2nd record in the nested table (2nd level)
        SecondLevelNestedTableInterface secondLevelNestedTable2 = builder.build(SecondLevelNestedTableInterface.class);
        secondLevelNestedTable2.setByteRequiredColumn((byte)8);
        secondLevelNestedTableInterfaceList.add(secondLevelNestedTable2);

        //3. create a nested table at the 1st level
        List<FirstLevelNestedTableInterface> firstLevelNestedTableInterfaceList = new ArrayList<>();

        Date today = new Date();
        long ts = today.getTime();
        FirstLevelNestedTableInterface firstLevelNestedTableInterface1 = builder.build(FirstLevelNestedTableInterface.class);
        firstLevelNestedTableInterface1.setEventTime(ts);
        firstLevelNestedTableInterface1.setIntRequiredColumn(111);
        firstLevelNestedTableInterface1.setNestedTableColumn(secondLevelNestedTableInterfaceList);
        firstLevelNestedTableInterfaceList.add(firstLevelNestedTableInterface1);

        firstLevelNestedTableInterface1 = builder.build(FirstLevelNestedTableInterface.class);
        firstLevelNestedTableInterface1.setEventTime(ts + 100000000000l);
        firstLevelNestedTableInterface1.setIntRequiredColumn(123);
        firstLevelNestedTableInterfaceList.add(firstLevelNestedTableInterface1);

        //4. create a record at the 1st level
        StructTableInterface sti = builder.build(StructTableInterface.class);
        sti.setIntSortedColumn(2345);
        sti.setNestedTableColumn(secondLevelNestedTableInterfaceList);

        //5. fill the profile properties
        mainTable.setStructColumn(sti);
        mainTable.setNestedTableColumn(firstLevelNestedTableInterfaceList);

        return mainTable.getBaseObject();
    }

}
