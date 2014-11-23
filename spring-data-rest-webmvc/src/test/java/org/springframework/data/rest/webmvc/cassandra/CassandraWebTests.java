/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.rest.webmvc.cassandra;

import java.io.IOException;
import java.util.Arrays;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.rest.webmvc.AbstractWebIntegrationTests;
import org.springframework.hateoas.Link;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration tests for Cassandra repositories
 *
 * @author Greg Turnquist
 */
@ContextConfiguration(classes = CassandraRepoConfig.class)
public class CassandraWebTests extends AbstractCassandraIntegrationTest {

	@Autowired private EmployeeRepository repository;

	@Override
	protected Iterable<String> expectedRootLinkRels() {
		return Arrays.asList("employees");
	}

	@Before
	public void initDatabase() throws ConfigurationException, IOException, TTransportException, InterruptedException {

		Employee employee = new Employee();
		employee.setId("123");
		employee.setFirstName("Frodo");
		employee.setLastName("Baggins");
		employee.setTitle("ring bearer");
		repository.save(employee);
	}

	@Test
	public void simpleTest() throws Exception {

		Link employeeLink = client.discoverUnique("employees");
		ObjectMapper mapper = new ObjectMapper();
		Employee employee = new Employee();
		employee.setId("789");
		employee.setFirstName("Bilbo");
		employee.setLastName("Baggins");
		employee.setTitle("burgler");
		String bilboString = mapper.writeValueAsString(employee);

		MockHttpServletResponse response = postAndGet(employeeLink, bilboString, MediaType.APPLICATION_JSON);

		assertJsonPathEquals("$.firstName", "Bilbo", response);
		assertJsonPathEquals("$.lastName", "Baggins", response);
		assertJsonPathEquals("$.title", "burgler", response);
	}
}
