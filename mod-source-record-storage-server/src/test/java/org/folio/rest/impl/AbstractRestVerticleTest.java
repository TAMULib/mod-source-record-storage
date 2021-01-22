package org.folio.rest.impl;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.impl.ModTenantAPI.LOAD_SAMPLE_PARAMETER;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.testcontainers.containers.PostgreSQLContainer;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.reactivex.core.Vertx;

public abstract class AbstractRestVerticleTest {

  private static PostgreSQLContainer<?> postgresSQLContainer;

  static final String TENANT_ID = "diku";

  static final String SOURCE_STORAGE_RECORDS_PATH = "/source-storage/records";
  static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/source-storage/snapshots";
  static final String SOURCE_STORAGE_SOURCE_RECORDS_PATH = "/source-storage/source-records";

  static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  static final String RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawEdifactRecord.sample";
  static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  static final String PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedEdifactRecordContent.sample";
  static final String OKAPI_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6ImNjNWI3MzE3LWYyNDctNTYyMC1hYTJmLWM5ZjYxYjI5M2Q3NCIsImlhdCI6MTU3NzEyMTE4NywidGVuYW50IjoiZGlrdSJ9.0TDnGadsNpFfpsFGVLX9zep5_kIBJII2MU7JhkFrMRw";

  static Vertx vertx;
  static RequestSpecification spec;
  static RequestSpecification specWithoutUserId;

  private static String useExternalDatabase;
  private static int okapiPort;

  protected static final String FIRST_MARC_UUID = UUID.randomUUID().toString();
  protected static final String SECOND_MARC_UUID = UUID.randomUUID().toString();
  protected static final String THIRD_MARC_UUID = UUID.randomUUID().toString();
  protected static final String FOURTH_MARC_UUID = UUID.randomUUID().toString();
  protected static final String FIFTH_MARC_UUID = UUID.randomUUID().toString();
  protected static final String SIXTH_MARC_UUID = UUID.randomUUID().toString();
  protected static final String FIRST_EDIFACT_UUID = UUID.randomUUID().toString();
  protected static final String SECOND_EDIFACT_UUID = UUID.randomUUID().toString();
  protected static final String THIRD_EDIFACT_UUID = UUID.randomUUID().toString();
  protected static final String FIFTH_EDIFACT_UUID = UUID.randomUUID().toString();

  protected static RawRecord rawMarcRecord;
  protected static ParsedRecord edifactRecord;
  protected static RawRecord rawEdifactRecord;
  protected static ParsedRecord marcRecord;

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

  protected static ParsedRecord invalidParsedRecord = new ParsedRecord()
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  protected static ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  protected static Snapshot marc_snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  protected static Snapshot marc_snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  protected static Snapshot edifact_snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  protected static Snapshot edifact_snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  protected static Record marc_record_1 = new Record()
    .withId(FIRST_MARC_UUID)
    .withSnapshotId(marc_snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIRST_MARC_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  protected static Record marc_record_2 = new Record()
    .withId(SECOND_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(SECOND_MARC_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString()));
  protected static Record marc_record_3 = new Record()
    .withId(THIRD_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_MARC_UUID)
    .withState(Record.State.ACTUAL);
  protected static Record marc_record_4 = new Record()
    .withId(FOURTH_MARC_UUID)
    .withSnapshotId(marc_snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FOURTH_MARC_UUID)
    .withOrder(1)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid("12345"));
  protected static Record marc_record_5 = new Record()
    .withId(FIFTH_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIFTH_MARC_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL);
  protected static Record marc_record_6 = new Record()
    .withId(SIXTH_MARC_UUID)
    .withSnapshotId(marc_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(SIXTH_MARC_UUID)
    .withParsedRecord(marcRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid("12345"));
  protected static Record edifact_record_1 = new Record()
    .withId(FIRST_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withMatchedId(FIRST_EDIFACT_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  protected static Record edifact_record_2 = new Record()
    .withId(SECOND_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withParsedRecord(edifactRecord)
    .withMatchedId(SECOND_EDIFACT_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL);
  protected static Record edifact_record_3 = new Record()
    .withId(THIRD_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_EDIFACT_UUID)
    .withState(Record.State.ACTUAL);
  protected static Record edifact_record_5 = new Record()
    .withId(FIFTH_EDIFACT_UUID)
    .withSnapshotId(edifact_snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withMatchedId(FIFTH_EDIFACT_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL);

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    okapiPort = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + okapiPort;

    useExternalDatabase = System.getProperty(
      "org.folio.source.storage.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.source.storage.test.config",
          "/postgres-conf-local.json");
        PostgresClientFactory.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        String postgresImage = PomReader.INSTANCE.getProps().getProperty("postgres.image");
        postgresSQLContainer = new PostgreSQLContainer<>(postgresImage);
        postgresSQLContainer.start();
    
        Envs.setEnv(
          postgresSQLContainer.getHost(),
          postgresSQLContainer.getFirstMappedPort(),
          postgresSQLContainer.getUsername(),
          postgresSQLContainer.getPassword(),
          postgresSQLContainer.getDatabaseName()
        );
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.source.storage.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient(okapiUrl, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", okapiPort));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, res -> {
      try {
        tenantClient.postTenant(new TenantAttributes()
          .withModuleTo("1.0")
          .withParameters(Collections.singletonList(new Parameter()
            .withKey(LOAD_SAMPLE_PARAMETER)
            .withValue("true"))), res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @Before
  public void setUp() {
    String okapiUserId = UUID.randomUUID().toString();
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .setBaseUri("http://localhost:" + okapiPort)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port())
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, okapiUserId)
      .build();

    specWithoutUserId = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .setBaseUri("http://localhost:" + okapiPort)
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_HEADER_TOKEN, OKAPI_TOKEN)
      .build();
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        postgresSQLContainer.stop();
      }
      async.complete();
    }));
  }

  protected void postSnapshots(TestContext testContext, Snapshot... snapshots) {
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

  protected void postRecords(TestContext testContext, Record... records) {
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
