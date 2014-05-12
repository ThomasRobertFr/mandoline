/**
 * @author Cindy Roullet
 */
package fr.solap4py.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ IntegrationTest.class, JSONBuilderTest.class, MDXBuilderTest.class, MetadataTest.class, Solap4pyTest.class })
public class AllTests {
}
