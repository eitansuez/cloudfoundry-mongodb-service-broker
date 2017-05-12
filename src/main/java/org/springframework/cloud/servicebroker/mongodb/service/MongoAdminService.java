package org.springframework.cloud.servicebroker.mongodb.service;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.servicebroker.mongodb.exception.MongoServiceException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for manipulating a Mongo database.
 *
 * @author sgreenberg@pivotal.io
 */
@Service
public class MongoAdminService {

  private Logger logger = LoggerFactory.getLogger(MongoAdminService.class);

  private MongoClient client;

  @Autowired
  public MongoAdminService(MongoClient client) {
    this.client = client;
  }

  boolean databaseExists(String databaseName) throws MongoServiceException {
    try {
      List<String> databaseNames = new ArrayList<>();
      client.listDatabaseNames().into(databaseNames);
      return databaseNames.contains(databaseName);
    } catch (MongoException e) {
      throw handleException(e);
    }
  }

  void deleteDatabase(String databaseName) throws MongoServiceException {
    try {
      client.dropDatabase(databaseName);
    } catch (MongoException e) {
      throw handleException(e);
    }
  }

  MongoDatabase createDatabase(String databaseName) throws MongoServiceException {
    try {
      MongoDatabase db = client.getDatabase(databaseName);

      // save into a collection to force DB creation.
      db.createCollection("foo");
      db.getCollection("foo").insertOne(new Document("foo", "bar"));

      return db;
    } catch (MongoException e) {
      // try to clean up and fail
      try {
        deleteDatabase(databaseName);
      } catch (MongoServiceException ignore) {
      }
      throw handleException(e);
    }
  }

  void createUser(String database, String username, String password) throws MongoServiceException {
    try {
      Document createUserCmd = new Document("createUser", username)
          .append("pwd", password)
          .append("roles", Collections.singletonList("readWrite"));

      MongoDatabase db = client.getDatabase(database);
      Document result = db.runCommand(createUserCmd);
      boolean success = (result.getDouble("ok") == 1.0); // this is awful! i need to learn to interpret command results properly
      if (!success) {
        MongoServiceException e = new MongoServiceException(result.toString());
        logger.warn(e.getLocalizedMessage());
        throw e;
      }
    } catch (MongoException e) {
      throw handleException(e);
    }
  }

  void deleteUser(String database, String username) throws MongoServiceException {
    try {
      MongoDatabase db = client.getDatabase(database);
      db.runCommand(new Document("dropUser", username));
    } catch (MongoException e) {
      throw handleException(e);
    }
  }

  String getConnectionString(String database, String username, String password) {
    return String.format("mongodb://%s:%s@%s/%s", username, password, getServerAddresses(), database);
  }

  private String getServerAddresses() {
    StringBuilder builder = new StringBuilder();
    for (ServerAddress address : client.getAllAddress()) {
      builder.append(address.getHost())
          .append(":")
          .append(address.getPort())
          .append(",");
    }
    if (builder.length() > 0) {
      builder.deleteCharAt(builder.length() - 1);
    }
    return builder.toString();
  }

  private MongoServiceException handleException(Exception e) {
    logger.warn(e.getLocalizedMessage(), e);
    return new MongoServiceException(e.getLocalizedMessage());
  }

}
