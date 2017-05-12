package org.springframework.cloud.servicebroker.mongodb.service;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.servicebroker.mongodb.exception.MongoServiceException;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.springframework.cloud.servicebroker.mongodb.Fixtures.DB_NAME;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoAdminServiceIntegrationTest {

  @Autowired
  private MongoAdminService service;

  @Autowired
  private MongoClient client;

  @After
  public void cleanup() {
    client.getDatabase(DB_NAME).runCommand(new Document("dropAllUsersFromDatabase", ""));
    client.dropDatabase(DB_NAME);
  }

  @Test
  public void instanceCreationIsSuccessful() throws MongoServiceException {
    MongoDatabase db = service.createDatabase(DB_NAME);
    List<String> names = new ArrayList<>();
    client.listDatabaseNames().into(names);
    assertTrue(names.contains(DB_NAME));
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
    List<String> names = new ArrayList<>();
    client.listDatabaseNames().into(names);
    assertTrue(names.contains(DB_NAME));

    service.deleteDatabase(DB_NAME);
    names = new ArrayList<>();
    names = client.listDatabaseNames().into(names);
    assertFalse(names.contains(DB_NAME));
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

  @Test(expected = MongoTimeoutException.class)
  public void deleteUserSucceeds() throws MongoServiceException {
    service.createDatabase(DB_NAME);
    Document createUserCmd = new Document("createUser", "user").append("pwd", "password")
        .append("roles", new BasicDBList());

    Document result = client.getDatabase(DB_NAME).runCommand(createUserCmd);
    boolean ok = (result.getDouble("ok") == 1.0); // TODO: fix interpretation of ok
    assertTrue("create should succeed", ok);
    service.deleteUser(DB_NAME, "user");

    new MongoClient(
        singletonList(new ServerAddress("localhost", 27017)),
        singletonList(MongoCredential.createCredential("user", DB_NAME, "password".toCharArray())),
        new MongoClientOptions.Builder().serverSelectionTimeout(500).build()
    ).getDatabase(DB_NAME).listCollectionNames().first();
  }

}

