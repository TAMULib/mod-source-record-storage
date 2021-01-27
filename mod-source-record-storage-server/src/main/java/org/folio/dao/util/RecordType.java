package org.folio.dao.util;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByType;
import static org.folio.rest.jooq.Tables.EDIFACT_RECORDS_LB;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_LB;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.util.Objects;
import java.util.UUID;

import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.LoaderOptionsStep;
import org.jooq.Record2;

/**
 * Enum used to distingush table for parsed record. Used to convert
 * {@link Record} type to parsed record database table.
 * 
 * Enum string value must match those of
 * {@link org.folio.rest.jaxrs.model.Record.RecordType}.
 */
public enum RecordType implements ParsedRecordType {

  MARC("marc_records_lb") {
    @Override
    public void formatRecord(Record record) throws Exception {
      if (Objects.nonNull(record.getRecordType()) &&
          Objects.nonNull(record.getParsedRecord()) &&
          Objects.nonNull(record.getParsedRecord().getContent())) {
        String content = ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord());
        record.getParsedRecord().setFormattedContent(MarcUtil.marcJsonToTxtMarc(content));
      }
    }

    @Override
    public Condition getRecordImplicitCondition() {
      return filterRecordByType(this.name());
    }

    @Override
    public Condition getSourceRecordImplicitCondition() {
      return filterRecordByType(this.name())
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull());
    }

    @Override
    public Record2<UUID, JSONB> toDatabaseRecord2(ParsedRecord parsedRecord) {
      return ParsedRecordDaoUtil.toDatabaseMarcRecord(parsedRecord);
    }

    @Override
    public LoaderOptionsStep<?> toLoaderOptionsStep(DSLContext dsl) {
      return dsl.loadInto(MARC_RECORDS_LB);
    }
  },

  EDIFACT("edifact_records_lb") {
    @Override
    public void formatRecord(Record record) throws Exception {
      // NOTE: formatting EDIFACT raw record
      if (Objects.nonNull(record.getRecordType()) &&
          Objects.nonNull(record.getParsedRecord()) &&
          Objects.nonNull(record.getRawRecord()) &&
          Objects.nonNull(record.getRawRecord().getContent())) {
        String content = record.getRawRecord().getContent();
        record.getParsedRecord().setFormattedContent(EdifactUtil.formatEdifact(content));
      }
    }

    @Override
    public Condition getRecordImplicitCondition() {
      return filterRecordByType(this.name());
    }

    @Override
    public Condition getSourceRecordImplicitCondition() {
      return filterRecordByType(this.name());
    }

    @Override
    public Record2<UUID, JSONB> toDatabaseRecord2(ParsedRecord parsedRecord) {
      return ParsedRecordDaoUtil.toDatabaseEdifactRecord(parsedRecord);
    }

    @Override
    public LoaderOptionsStep<?> toLoaderOptionsStep(DSLContext dsl) {
      return dsl.loadInto(EDIFACT_RECORDS_LB);
    }
  };

  private String tableName;

  RecordType(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

}
