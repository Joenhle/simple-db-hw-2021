package simpledb.systemtest;

import org.junit.Before;

import simpledb.common.Database;

/**
 * Base class for all SimpleDb scantest classes.
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each scantest is run.
	 */
	@Before	public void setUp() throws Exception {
		Database.reset();
	}
	
}
