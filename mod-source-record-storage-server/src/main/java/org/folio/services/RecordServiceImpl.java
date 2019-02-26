package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.RecordDao;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

@Component
public class RecordServiceImpl implements RecordService {

  @Autowired
  private RecordDao recordDao;

  @Override
  public Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    return recordDao.getRecords(query, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return recordDao.getRecordById(id, tenantId);
  }

  @Override
  public Future<Boolean> saveRecord(Record record, String tenantId) {
    if (record.getId() == null) {
      record.setId(UUID.randomUUID().toString());
    }
    record.getRawRecord().setId(UUID.randomUUID().toString());
    if (record.getParsedRecord() != null) {
      record.getParsedRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getErrorRecord() != null) {
      record.getErrorRecord().setId(UUID.randomUUID().toString());
    }
    return recordDao.saveRecord(record, tenantId);
  }


  @Override
  public Future<Boolean> updateRecord(Record record, String tenantId) {
    return getRecordById(record.getId(), tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(r -> {
          ParsedRecord parsedRecord = record.getParsedRecord();
          if (parsedRecord != null && (parsedRecord.getId() == null || parsedRecord.getId().isEmpty())) {
            parsedRecord.setId(UUID.randomUUID().toString());
          }
          ErrorRecord errorRecord = record.getErrorRecord();
          if (errorRecord != null && (errorRecord.getId() == null || errorRecord.getId().isEmpty())) {
            errorRecord.setId(UUID.randomUUID().toString());
          }
          return recordDao.updateRecord(record, tenantId);
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", record.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteRecord(String id, String tenantId) {
    return recordDao.deleteRecord(id, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, String tenantId) {
    return recordDao.getSourceRecords(query, offset, limit, tenantId);
  }

}