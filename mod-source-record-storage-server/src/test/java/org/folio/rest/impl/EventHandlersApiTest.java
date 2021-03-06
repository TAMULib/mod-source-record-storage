package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EventHandlersApiTest extends AbstractRestVerticleTest {

  public static final String HANDLERS_UPDATED_RECORD_PATH = "/source-storage/handlers/updated-record";

  private JsonObject event = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("eventMetadata", new JsonObject()
      .put("tenantId", TENANT_ID)
      .put("eventTTL", 1)
      .put("publishedBy", "mod-inventory"))
    .put("context", new JsonObject());

  @Test
  public void shouldReturnNoContentWhenReceivedRecordUpdatedEvent() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(event.put("eventType", "QM_MARC_BIB_RECORD_UPDATED").encode())
      .post(HANDLERS_UPDATED_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

}
