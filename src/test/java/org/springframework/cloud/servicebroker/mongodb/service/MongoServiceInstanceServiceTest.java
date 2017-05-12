package org.springframework.cloud.servicebroker.mongodb.service;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.servicebroker.exception.ServiceBrokerException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceDoesNotExistException;
import org.springframework.cloud.servicebroker.exception.ServiceInstanceExistsException;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.CreateServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceRequest;
import org.springframework.cloud.servicebroker.model.DeleteServiceInstanceResponse;
import org.springframework.cloud.servicebroker.model.ServiceDefinition;
import org.springframework.cloud.servicebroker.mongodb.Fixtures;
import org.springframework.cloud.servicebroker.mongodb.model.ServiceInstance;
import org.springframework.cloud.servicebroker.mongodb.repository.MongoServiceInstanceRepository;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.cloud.servicebroker.mongodb.Fixtures.DB_NAME;


@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoServiceInstanceServiceTest {

  private static final String SVC_DEF_ID = "serviceDefinitionId";
  private static final String SVC_PLAN_ID = "servicePlanId";

  @Autowired
  private MongoClient client;

  @Mock
  private MongoAdminService mongo;

  @Mock
  private MongoServiceInstanceRepository repository;

  @Mock
  private MongoDatabase db;

  @Mock
  private ServiceDefinition serviceDefinition;

  private MongoServiceInstanceService service;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    service = new MongoServiceInstanceService(mongo, repository);
  }

  @After
  public void cleanup() {
    client.dropDatabase(DB_NAME);
  }

  @Test
  public void newServiceInstanceCreatedSuccessfully() throws Exception {

    when(repository.findOne(any(String.class))).thenReturn(null);
    when(mongo.databaseExists(any(String.class))).thenReturn(false);
    when(mongo.createDatabase(any(String.class))).thenReturn(db);

    CreateServiceInstanceResponse response = service.createServiceInstance(buildCreateRequest());

    assertNotNull(response);
    assertNull(response.getDashboardUrl());
    assertFalse(response.isAsync());

    verify(repository).save(isA(ServiceInstance.class));
  }

  @Test
  public void newServiceInstanceCreatedSuccessfullyWithExistingDB() throws Exception {

    when(repository.findOne(any(String.class))).thenReturn(null);
    when(mongo.databaseExists(any(String.class))).thenReturn(true);
    when(mongo.createDatabase(any(String.class))).thenReturn(db);

    CreateServiceInstanceRequest request = buildCreateRequest();
    CreateServiceInstanceResponse response = service.createServiceInstance(request);

    assertNotNull(response);
    assertNull(response.getDashboardUrl());
    assertFalse(response.isAsync());

    verify(mongo).deleteDatabase(request.getServiceInstanceId());
    verify(repository).save(isA(ServiceInstance.class));
  }

  @Test(expected = ServiceInstanceExistsException.class)
  public void serviceInstanceCreationFailsWithExistingInstance() throws Exception {
    when(repository.findOne(any(String.class))).thenReturn(Fixtures.getServiceInstance());

    service.createServiceInstance(buildCreateRequest());
  }

  @Test(expected = ServiceBrokerException.class)
  public void serviceInstanceCreationFailsWithDBCreationFailure() throws Exception {
    when(repository.findOne(any(String.class))).thenReturn(null);
    when(mongo.databaseExists(any(String.class))).thenReturn(false);
    when(mongo.createDatabase(any(String.class))).thenReturn(null);

    service.createServiceInstance(buildCreateRequest());
  }

  @Test
  public void successfullyRetrieveServiceInstance() {
    when(repository.findOne(any(String.class))).thenReturn(Fixtures.getServiceInstance());
    String id = Fixtures.getServiceInstance().getServiceInstanceId();
    assertEquals(id, service.getServiceInstance(id).getServiceInstanceId());
  }

  @Test
  public void serviceInstanceDeletedSuccessfully() throws Exception {
    ServiceInstance instance = Fixtures.getServiceInstance();
    when(repository.findOne(any(String.class))).thenReturn(instance);
    String id = instance.getServiceInstanceId();

    DeleteServiceInstanceResponse response = service.deleteServiceInstance(buildDeleteRequest());

    assertNotNull(response);
    assertFalse(response.isAsync());

    verify(mongo).deleteDatabase(id);
    verify(repository).delete(id);
  }

  @Test(expected = ServiceInstanceDoesNotExistException.class)
  public void unknownServiceInstanceDeleteCallSuccessful() throws Exception {
    when(repository.findOne(any(String.class))).thenReturn(null);

    DeleteServiceInstanceRequest request = buildDeleteRequest();

    DeleteServiceInstanceResponse response = service.deleteServiceInstance(request);

    assertNotNull(response);
    assertFalse(response.isAsync());

    verify(mongo).deleteDatabase(request.getServiceInstanceId());
    verify(repository).delete(request.getServiceInstanceId());
  }

  private CreateServiceInstanceRequest buildCreateRequest() {
    return new CreateServiceInstanceRequest(SVC_DEF_ID, SVC_PLAN_ID, "organizationGuid", "spaceGuid")
        .withServiceInstanceId(Fixtures.getServiceInstance().getServiceInstanceId());
  }

  private DeleteServiceInstanceRequest buildDeleteRequest() {
    return new DeleteServiceInstanceRequest(Fixtures.getServiceInstance().getServiceInstanceId(),
        SVC_DEF_ID, SVC_PLAN_ID, serviceDefinition);
  }
}