package fr.solap4py.core;

import junit.framework.JUnit4TestAdapter;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ IntegrationTest.class, JSONBuilderTest.class,
		MDXBuilderTest.class, MetadataTest.class, Solap4pyTest.class })
public class AllTests {
	 // Execution begins at main().  In this test class, we will execute
    // a text test runner that will tell you if any of your tests fail.
    public static void main (String[] args) 
    {
       junit.textui.TestRunner.run (suite());
    }

    // The suite() method is helpful when using JUnit 3 Test Runners or Ant.
    public static junit.framework.Test suite() 
    {
       return new JUnit4TestAdapter(AllTests.class);
    }
	
	
	
}

