package edu.utsa.testmobbed;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ TestAttributes.class, TestDatadefs.class, TestElements.class,
		TestEvents.class, TestEventTypes.class, TestManageDB.class,
		TestMetadata.class, TestNumericStreams.class, TestStructures.class })
public class AllTests {

}
