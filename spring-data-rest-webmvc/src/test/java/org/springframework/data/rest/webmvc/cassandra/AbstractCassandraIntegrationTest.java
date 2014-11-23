package org.springframework.data.rest.webmvc.cassandra;

import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.BeforeClass;
import org.springframework.data.rest.webmvc.AbstractWebIntegrationTests;

public abstract class AbstractCassandraIntegrationTest extends AbstractWebIntegrationTests {

	/**
	 * The session connected to the system keyspace.
	 */
	protected Session systemSession;
	/**
	 * The {@link com.datastax.driver.core.Cluster} that's connected to Cassandra.
	 */
	protected Cluster cluster;

	/**
	 * Launch an embedded Cassandra instance
	 *
	 * @throws ConfigurationException
	 * @throws IOException
	 * @throws TTransportException
	 */
	@BeforeClass
	public static void startDatabase() throws ConfigurationException, IOException, TTransportException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	public AbstractCassandraIntegrationTest() {
		// check cluster
		if (cluster == null) {
			cluster = Cluster.builder()//
					.addContactPoint(CassandraProperties.HOSTNAME)//
					.withPort(CassandraProperties.PORT)//
					.build();
		}

		// check system session connected
		if (systemSession == null) {
			systemSession = cluster.connect();
		}
	}

}
