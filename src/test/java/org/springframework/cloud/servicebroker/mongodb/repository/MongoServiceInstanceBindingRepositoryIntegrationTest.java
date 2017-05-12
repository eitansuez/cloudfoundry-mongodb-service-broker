package org.springframework.cloud.servicebroker.mongodb.repository;

import static org.junit.Assert.assertEquals;
import static org.springframework.cloud.servicebroker.mongodb.Fixtures.DB_NAME;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.servicebroker.mongodb.Fixtures;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;

import com.mongodb.MongoClient;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoServiceInstanceBindingRepositoryIntegrationTest {

	private static final String COLLECTION = "serviceInstanceBinding";

	@Autowired
	private MongoClient client;

	@Autowired
	private MongoServiceInstanceBindingRepository repository;

	@Autowired
	private MongoOperations mongo;

	@Before
	public void setup() throws Exception {
		mongo.dropCollection(COLLECTION);
	}

	@After
	public void teardown() {
		mongo.dropCollection(COLLECTION);
		client.dropDatabase(DB_NAME);
	}

	@Test
	public void bindingInsertedSuccessfully() throws Exception {
		assertEquals(0, mongo.getCollection(COLLECTION).count());
		repository.save(Fixtures.getServiceInstanceBinding());
		assertEquals(1, mongo.getCollection(COLLECTION).count());
	}

	@Test
	public void bindingDeletedSuccessfully() throws Exception {
		assertEquals(0, mongo.getCollection(COLLECTION).count());
		repository.save(Fixtures.getServiceInstanceBinding());
		assertEquals(1, mongo.getCollection(COLLECTION).count());
		repository.delete(Fixtures.getServiceInstanceBinding().getId());
		assertEquals(0, mongo.getCollection(COLLECTION).count());
	}
}