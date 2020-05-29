package org.folio.dao;

import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;

import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.SnapshotsLb;
import org.folio.rest.jooq.tables.records.SnapshotsLbRecord;
import org.jooq.Condition;
import org.jooq.InsertSetStep;
import org.jooq.InsertValuesStepN;
import org.jooq.OrderField;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class LBSnapshotDao {

  public static Future<List<Snapshot>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.executeAny(dsl -> dsl.selectFrom(SNAPSHOTS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(LBSnapshotDao::toSnapshots);
  }

  public static Future<Optional<Snapshot>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(SNAPSHOTS_LB)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(id))))
        .map(LBSnapshotDao::toOptionalSnapshot);
  }

  public static Future<Snapshot> save(ReactiveClassicGenericQueryExecutor queryExecutor, Snapshot snapshot) {
    SnapshotsLbRecord record = toDatabaseRecord(snapshot);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(SNAPSHOTS_LB)
      .set(record)
      .onDuplicateKeyUpdate()
      .set(record)
      .returning())
        .map(LBSnapshotDao::toSnapshot);
  }

  public static Future<List<Snapshot>> save(ReactiveClassicGenericQueryExecutor queryExecutor, List<Snapshot> snapshots) {
    return queryExecutor.executeAny(dsl -> {
      InsertSetStep<SnapshotsLbRecord> insertSetStep = dsl.insertInto(SNAPSHOTS_LB);
      InsertValuesStepN<SnapshotsLbRecord> insertValuesStepN = null;
      for (Snapshot snapshot : snapshots) {
          insertValuesStepN = insertSetStep.values(toDatabaseRecord(snapshot).intoArray());
      }
      return insertValuesStepN;
    }).map(LBSnapshotDao::toSnapshots);
  }

  public static Future<Snapshot> update(ReactiveClassicGenericQueryExecutor queryExecutor, Snapshot snapshot) {
    SnapshotsLbRecord record = toDatabaseRecord(setProcessingStartedDate(snapshot));
    return queryExecutor.executeAny(dsl -> dsl.update(SNAPSHOTS_LB)
      .set(record)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(snapshot.getJobExecutionId())))
      .returning())
        .map(LBSnapshotDao::toOptionalSnapshot)
        .map(optionalSnapshot -> {
          if (optionalSnapshot.isPresent()) {
            return optionalSnapshot.get();
          }
          throw new RuntimeException(String.format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId()));
        });
  }

  public static Future<Boolean> delete(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(SNAPSHOTS_LB)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1);
  }

  public static Snapshot toSnapshot(Row row) {
    SnapshotsLb pojo = RowMappers.getSnapshotsLbMapper().apply(row);
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(pojo.getId().toString())
      .withStatus(Status.fromValue(pojo.getStatus().toString()));
    if (Objects.nonNull(pojo.getProcessingStartedDate())) {
      snapshot.withProcessingStartedDate(Date.from(pojo.getProcessingStartedDate().toInstant()));
    }
    Metadata metadata = new Metadata();
    if (Objects.nonNull(pojo.getCreatedByUserId())) {
      metadata.withCreatedByUserId(pojo.getCreatedByUserId().toString());
    }
    if (Objects.nonNull(pojo.getCreatedDate())) {
      metadata.withCreatedDate(Date.from(pojo.getCreatedDate().toInstant()));
    }
    if (Objects.nonNull(pojo.getUpdatedByUserId())) {
      metadata.withUpdatedByUserId(pojo.getUpdatedByUserId().toString());
    }
    if (Objects.nonNull(pojo.getUpdatedDate())) {
      metadata.withUpdatedDate(Date.from(pojo.getUpdatedDate().toInstant()));
    }
    return snapshot.withMetadata(metadata);
  }

  public static SnapshotsLbRecord toDatabaseRecord(Snapshot snapshot) {
    SnapshotsLbRecord record = new SnapshotsLbRecord();
    if (StringUtils.isNotEmpty(snapshot.getJobExecutionId())) {
      record.setId(UUID.fromString(snapshot.getJobExecutionId()));
    }
    if (Objects.nonNull(snapshot.getStatus())) {
      record.setStatus(JobExecutionStatus.valueOf(snapshot.getStatus().toString()));
    }
    if (Objects.nonNull(snapshot.getProcessingStartedDate())) {
      record.setProcessingStartedDate(snapshot.getProcessingStartedDate().toInstant().atOffset(ZoneOffset.UTC));
    }
    if (Objects.nonNull(snapshot.getMetadata())) {
      if (Objects.nonNull(snapshot.getMetadata().getCreatedByUserId())) {
        record.setCreatedByUserId(UUID.fromString(snapshot.getMetadata().getCreatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getCreatedDate())) {
        record.setCreatedDate(snapshot.getMetadata().getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedByUserId())) {
        record.setUpdatedByUserId(UUID.fromString(snapshot.getMetadata().getUpdatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedDate())) {
        record.setUpdatedDate(snapshot.getMetadata().getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
    }
    return record;
  }

  private static Snapshot toSnapshot(RowSet<Row> rows) {
    return toSnapshot(rows.iterator().next());
  }

  private static List<Snapshot> toSnapshots(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false).map(LBSnapshotDao::toSnapshot).collect(Collectors.toList());
  }

  private static Optional<Snapshot> toOptionalSnapshot(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toSnapshot(rows.iterator().next())) : Optional.empty();
  }

  private static Optional<Snapshot> toOptionalSnapshot(Row row) {
    return Objects.nonNull(row) ? Optional.of(toSnapshot(row)) : Optional.empty();
  }

  private static Snapshot setProcessingStartedDate(Snapshot snapshot) {
    if (Snapshot.Status.PARSING_IN_PROGRESS.equals(snapshot.getStatus())) {
      snapshot.setProcessingStartedDate(new Date());
    }
    return snapshot;
  }

}