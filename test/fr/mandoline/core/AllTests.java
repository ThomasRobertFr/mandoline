/**
 * @author Cindy Roullet
 * @version 1.00
 */
package fr.mandoline.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ IntegrationTest.class, JSONBuilderTest.class, MDXBuilderTest.class, MetadataTest.class, MandolineTest.class })
public class AllTests {
}
