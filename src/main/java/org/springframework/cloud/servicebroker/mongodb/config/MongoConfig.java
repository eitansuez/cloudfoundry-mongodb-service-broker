package org.springframework.cloud.servicebroker.mongodb.config;

import com.mongodb.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.net.UnknownHostException;
import java.util.Collections;

@Configuration
@EnableMongoRepositories(basePackages = "org.springframework.cloud.servicebroker.mongodb.repository")
public class MongoConfig extends AbstractMongoConfiguration {

  @Override
  protected String getDatabaseName() {
    return "mongodb-service-broker";
  }

  @Value("${mongodb.host:localhost}")
  private String host;

  @Value("${mongodb.port:27017}")
  private int port;

  @Value("${mongodb.user:root}")
  private String username;

  @Value("${mongodb.password:pass}")
  private String password;

  @Bean
  public MongoClient mongoClient() throws UnknownHostException {
    MongoCredential credential = MongoCredential.createScramSha1Credential(username, "admin", password.toCharArray());
    return new MongoClient(new ServerAddress(host, port), Collections.singletonList(credential));
  }

  @Override
  public Mongo mongo() throws Exception {
    return mongoClient();
  }

}
