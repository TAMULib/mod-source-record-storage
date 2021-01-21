package org.folio.rest.impl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RecordApiTest extends AbstractRestVerticleTest {

  private static final String FIRST_MARC_UUID = UUID.randomUUID().toString();
  private static final String SECOND_MARC_UUID = UUID.randomUUID().toString();
  private static final String THIRD_MARC_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_MARC_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_MARC_UUID = UUID.randomUUID().toString();
  private static final String FIRST_EDIFACT_UUID = UUID.randomUUID().toString();
  private static final String SECOND_EDIFACT_UUID = UUID.randomUUID().toString();
  private static final String THIRD_EDIFACT_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_EDIFACT_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_EDIFACT_UUID = UUID.randomUUID().toString();
  private static final String SIXTH_EDIFACT_UUID = UUID.randomUUID().toString();

  private static RawRecord rawMarcRecord;
  private static ParsedRecord edifactRecord;
  private static RawRecord rawEdifactRecord;
  private static ParsedRecord marcRecord;

  static {
    try {
      rawEdifactRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), String.class));
      edifactRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
      rawMarcRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class));
      marcRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static ParsedRecord invalidParsedRecord = new ParsedRecord()
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  private static ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  private static Snapshot marc_snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot marc_snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot marc_snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot edifact_snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot edifact_snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot edifact_snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Record marc_record_1 = new Record()
    .withId(FIRST_MARC_UUID)
    .withSnapshotId(marc_snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIRST_MARC_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record marc_record_2 = new Record()
    .withId(SECOND_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(SECOND_MARC_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL);
  private static Record marc_record_3 = new Record()
    .withId(THIRD_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_MARC_UUID)
    .withState(Record.State.ACTUAL);
  private static Record marc_record_5 = new Record()
    .withId(FIFTH_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIFTH_MARC_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL);
    private static Record edifact_record_1 = new Record()
    .withId(FIRST_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withMatchedId(FIRST_EDIFACT_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record edifact_record_2 = new Record()
    .withId(SECOND_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withParsedRecord(edifactRecord)
    .withMatchedId(SECOND_EDIFACT_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL);
  private static Record edifact_record_3 = new Record()
    .withId(THIRD_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_EDIFACT_UUID)
    .withState(Record.State.ACTUAL);
  private static Record edifact_record_5 = new Record()
    .withId(FIFTH_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withMatchedId(FIFTH_EDIFACT_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL);

  @Before
  public void setUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoRecordsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("records", empty());
  }

  @Test
  public void shouldReturnAllRecordsWithNotEmptyStateOnGetWithSpecifiedTypeOrDefault(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(marc_snapshot_1, marc_snapshot_2, edifact_snapshot_1);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();

    Record marc_record_4 = new Record()
      .withId(FOURTH_MARC_UUID)
      .withSnapshotId(marc_snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_MARC_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(marc_record_1, marc_record_2, marc_record_3, marc_record_4, edifact_record_1);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    // Get with default recordType
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(4))
      .body("records*.state", everyItem(notNullValue()))
      .body("records*.recordType", everyItem(is(Record.RecordType.MARC.name())));
    async.complete();

    // Get with MARC recordType
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=MARC")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(4))
      .body("records*.state", everyItem(notNullValue()))
      .body("records*.recordType", everyItem(is(Record.RecordType.MARC.name())));
    async.complete();

    // Get with EDIFACT recordType
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=EDIFACT")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("records*.state", everyItem(notNullValue()))
      .body("records*.recordType", everyItem(is(Record.RecordType.EDIFACT.name())));
    async.complete();
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(marc_snapshot_1, marc_snapshot_2);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_MARC_UUID)
      .withSnapshotId(marc_snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_MARC_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(marc_record_1, marc_record_2, marc_record_3, recordWithOldStatus);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?state=ACTUAL&snapshotId=" + marc_record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.snapshotId", everyItem(is(marc_record_2.getSnapshotId())))
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnLimitedCollectionWithActualStateOnGetWithLimit(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(marc_snapshot_1, marc_snapshot_2, edifact_snapshot_1);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_MARC_UUID)
      .withSnapshotId(marc_snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_MARC_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(marc_record_1, marc_record_2, marc_record_3, recordWithOldStatus, edifact_record_1);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?state=ACTUAL&limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(2))
      .body("totalRecords", is(3));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateMarcRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(marc_record_1.getSnapshotId()))
      .body("recordType", is(marc_record_1.getRecordType().name()))
      .body("rawRecord.content", is(rawMarcRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  public void shouldCreateEdifactRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(edifact_snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(edifact_record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(edifact_record_1.getSnapshotId()))
      .body("recordType", is(edifact_record_1.getRecordType().name()))
      .body("rawRecord.content", is(rawEdifactRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(marc_record_3.getSnapshotId()))
      .body("recordType", is(marc_record_3.getRecordType().name()))
      .body("rawRecord.content", is(rawMarcRecord.getContent()))
      .body("errorRecord.content", is(errorRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(marc_record_1)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingMarcRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(marc_record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setParsedRecord(marcRecord);
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    assertThat(updatedRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    ParsedRecord parsedRecord = updatedRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(updatedRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldUpdateExistingEdifactRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(edifact_snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(edifact_record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setParsedRecord(edifactRecord);
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    assertThat(updatedRecord.getRawRecord().getContent(), is(rawEdifactRecord.getContent()));
    ParsedRecord parsedRecord = updatedRecord.getParsedRecord();
    // TODO: replace this with EDIFACT parsed record contents
    // assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(updatedRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldUpdateErrorRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(marc_record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setErrorRecord(errorRecord);
    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdRecord.getId()))
      .body("rawRecord.content", is(createdRecord.getRawRecord().getContent()))
      .body("errorRecord.content", is(createdRecord.getErrorRecord().getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingRecordOnGetById(TestContext testContext) {
    postSnapshots(testContext, marc_snapshot_2, edifact_snapshot_2);
    postRecords(testContext, marc_record_2, edifact_record_2);

    Async async = testContext.async();
    Response getMarcResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + marc_record_2.getId());
    assertThat(getMarcResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getMarcRecord = getMarcResponse.body().as(Record.class);
    assertThat(getMarcRecord.getId(), is(marc_record_2.getId()));
    assertThat(getMarcRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    ParsedRecord parsedMarcRecord = getMarcRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedMarcRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(getMarcRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();

    async = testContext.async();
    Response getEdifactResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + marc_record_2.getId());
    assertThat(getEdifactResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getEdifactRecord = getEdifactResponse.body().as(Record.class);
    assertThat(getEdifactRecord.getId(), is(marc_record_2.getId()));
    assertThat(getEdifactRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    ParsedRecord parsedEdifactRecord = getEdifactRecord.getParsedRecord();
    // TODO: replace with parserd EDIFACT record contents
    // assertThat(JsonObject.mapFrom(parsedEdifactRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(getEdifactRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingRecordOnDelete(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(marc_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("deleted", is(true));
    async.complete();

    async = testContext.async();
    Response createErrorRecord = RestAssured.given()
      .spec(spec)
      .body(marc_record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createErrorRecord.statusCode(), is(HttpStatus.SC_CREATED));
    Record errorRecord = createErrorRecord.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + errorRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + errorRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("deleted", is(true));
    async.complete();
  }

  @Test
  public void shouldReturnSortedMarcRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, marc_snapshot_2);
    postRecords(testContext, marc_record_2, marc_record_3, marc_record_5);

    Async async = testContext.async();
    List<Record> records = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?snapshotId=" + marc_snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(3))
      .body("totalRecords", is(3))
      .body("records*.deleted", everyItem(is(false)))
      .extract().response().body().as(RecordCollection.class).getRecords();

    Assert.assertEquals(11, records.get(0).getOrder().intValue());
    Assert.assertEquals(101, records.get(1).getOrder().intValue());
    Assert.assertNull(records.get(2).getOrder());

    async.complete();
  }

  @Test
  public void shouldReturnSortedEdifactRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, edifact_snapshot_2);
    postRecords(testContext, edifact_record_2, edifact_record_3, edifact_record_5);

    Async async = testContext.async();
    List<Record> records = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=EDIFACT&snapshotId=" + edifact_snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(3))
      .body("totalRecords", is(3))
      .body("records*.deleted", everyItem(is(false)))
      .extract().response().body().as(RecordCollection.class).getRecords();

    Assert.assertEquals(11, records.get(0).getOrder().intValue());
    Assert.assertEquals(101, records.get(1).getOrder().intValue());
    Assert.assertNull(records.get(2).getOrder());

    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordIfParsedMarcContentIsInvalid(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(marc_record_5)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    assertThat(getRecord.getParsedRecord(), nullValue());
    assertThat(getRecord.getErrorRecord(), notNullValue());
    Assert.assertFalse(getRecord.getDeleted());
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordIfParsedEdifactContentIsInvalid(TestContext testContext) {
    postSnapshots(testContext, edifact_snapshot_2);
    postRecords(testContext, edifact_record_5);

    Async async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + edifact_record_5.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(edifact_record_5.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawEdifactRecord.getContent()));
    assertThat(getRecord.getParsedRecord(), nullValue());
    assertThat(getRecord.getErrorRecord(), notNullValue());
    Assert.assertFalse(getRecord.getDeleted());
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?state=error!")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?orderBy=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnCreatedRecordWithAdditionalInfoOnGetById(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(marc_snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(true));

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    ParsedRecord parsedRecord = getRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(newRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

  @Test
  public void suppressFromDiscoveryByInstanceIdSuccess(TestContext context) {
    Async async = context.async();
    RestAssured.given()
      .spec(spec)
      .body(marc_snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = context.async();
    String srsId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(srsId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields", new JsonArray().add(new JsonObject().put("s", srsId)).add(new JsonObject().put("i", instanceId)))))));

    Record newRecord = new Record()
      .withId(srsId)
      .withSnapshotId(marc_snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId))
      .withMatchedId(UUID.randomUUID().toString());

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(newRecord).toString())
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("id", is(srsId));
    async.complete();

    async = context.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + instanceId + "/suppress-from-discovery?idType=INSTANCE&suppress=true")
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();
  }

  @Test
  public void suppressFromDiscoveryByInstanceIdNotFound(TestContext context) {
    Async async = context.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + UUID.randomUUID().toString() + "/suppress-from-discovery?idType=INSTANCE&suppress=true")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  private void postSnapshots(TestContext testContext, Snapshot... snapshots) {
    Async async = testContext.async();
    for (Snapshot snapshot : snapshots) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();
  }

  private void postRecords(TestContext testContext, Record... records) {
    Async async = testContext.async();
    for (Record record : records) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();
  }

}
