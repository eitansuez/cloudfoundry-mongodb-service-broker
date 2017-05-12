package org.springframework.cloud.servicebroker.mongodb;

import org.springframework.cloud.servicebroker.mongodb.model.ServiceInstance;
import org.springframework.cloud.servicebroker.mongodb.model.ServiceInstanceBinding;

import java.util.Collections;
import java.util.Map;

public class Fixtures {

  public static final String DB_NAME = "test-mongo-db";

  public static ServiceInstance getServiceInstance() {
    return new ServiceInstance("service-instance-id", "service-definition-id", "plan-id",
        "org-guid", "space-guid", "http://dashboard.example.com");
  }

  public static ServiceInstanceBinding getServiceInstanceBinding() {
    Map<String, Object> credentials = Collections.singletonMap("url", (Object) "mongo://example.com");
    return new ServiceInstanceBinding("binding-id", "service-instance-id", credentials, null, "app-guid");
  }
}
