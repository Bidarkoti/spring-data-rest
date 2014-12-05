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

import static org.hamcrest.CoreMatchers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.hateoas.Link;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;

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
	public void cleanoutDatabase() throws ConfigurationException, IOException, TTransportException, InterruptedException {

		repository.deleteAll();
	}

	@Test
	public void postAndGet() throws Exception {

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

	@Test
	public void employeeSearch() throws Exception {


		Employee employee1 = new Employee();
		employee1.setId("123");
		employee1.setFirstName("Frodo");
		employee1.setLastName("Baggins");
		employee1.setTitle("ring bearer");
		repository.save(employee1);

		Employee employee2 = new Employee();
		employee2.setId("789");
		employee2.setFirstName("Samwise");
		employee2.setLastName("Gamgee");
		employee2.setTitle("ring bearer");
		repository.save(employee2);

		Link employeesLink = client.discoverUnique("employees");

		String results = client.follow(employeesLink)
				.andExpect(status().isOk())
				.andExpect(jsonPath("$._embedded.employees[*].firstName", hasItems("Samwise", "Frodo")))
				.andExpect(jsonPath("$._embedded.employees[*].lastName", hasItems("Gamgee", "Baggins")))
				.andExpect(jsonPath("$._embedded.employees[*].title", hasItems("ring bearer", "ring bearer")))
				.andReturn().getResponse().getContentAsString();
	}

}
