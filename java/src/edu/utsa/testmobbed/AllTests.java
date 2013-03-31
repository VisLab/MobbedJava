package edu.utsa.testmobbed;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestAttributes.class, TestDatasets.class, TestElements.class,
		TestEvents.class, TestEventTypes.class, TestEventTypeMaps.class,
		TestManageDB.class, TestMetadata.class, TestNumericStreams.class,
		TestStructures.class, })
public class AllTests {

}
