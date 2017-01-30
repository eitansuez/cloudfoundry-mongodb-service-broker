package org.springframework.cloud.servicebroker.mongodb.service;

import com.mongodb.*;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.mongodb.IntegrationTestBase;
import org.springframework.cloud.servicebroker.mongodb.exception.MongoServiceException;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;


public class MongoAdminServiceIntegrationTest extends IntegrationTestBase {
	
	@Autowired
	private MongoAdminService service;
	
	@Autowired
	private MongoClient client;
	
	@After
	public void cleanup() {
		client.getDB(DB_NAME).command("dropAllUsersFromDatabase");
		client.dropDatabase(DB_NAME);
	}

	@Test
	public void instanceCreationIsSuccessful() throws MongoServiceException {
		DB db = service.createDatabase(DB_NAME);
		assertTrue(client.getDatabaseNames().contains(DB_NAME));
		assertNotNull(db);
	}
	
	@Test
	public void databaseNameDoesNotExist() throws MongoServiceException {
		assertFalse(service.databaseExists("NOT_HERE"));
	}
	
	@Test
	public void databaseNameExists() throws MongoServiceException {
		service.createDatabase(DB_NAME);
		assertTrue(service.databaseExists(DB_NAME));
	}
	
	@Test
	public void deleteDatabaseSucceeds() throws MongoServiceException {
		service.createDatabase(DB_NAME);
		assertTrue(client.getDatabaseNames().contains(DB_NAME));
		service.deleteDatabase(DB_NAME);
		assertFalse(client.getDatabaseNames().contains(DB_NAME));
	}
	
	@Test
	public void newUserCreatedSuccessfully() throws MongoServiceException {
		service.createDatabase(DB_NAME);
		service.createUser(DB_NAME, "user", "password");

		// by querying db with new credentials, can vet that credentials work
		new MongoClient(
				singletonList(new ServerAddress("localhost", 27017)),
				singletonList(MongoCredential.createCredential("user", DB_NAME, "password".toCharArray()))
		).getDatabase(DB_NAME).listCollectionNames().first();

	}
	
	@Test(expected = MongoTimeoutException.class )
	public void deleteUserSucceeds() throws MongoServiceException {
		service.createDatabase(DB_NAME);
		DBObject createUserCmd = BasicDBObjectBuilder.start("createUser", "user").add("pwd", "password")
				.add("roles", new BasicDBList()).get();
		CommandResult result = client.getDB(DB_NAME).command(createUserCmd);
		assertTrue("create should succeed", result.ok());
		service.deleteUser(DB_NAME, "user");

		new MongoClient(
				singletonList(new ServerAddress("localhost", 27017)),
				singletonList(MongoCredential.createCredential("user", DB_NAME, "password".toCharArray())),
				new MongoClientOptions.Builder().serverSelectionTimeout(500).build()
		).getDatabase(DB_NAME).listCollectionNames().first();
	}
	
}

