package org.folio.rest.util;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.BadRequestException;

import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class QueryParamUtilTest {

  @Test
  public void shouldReturnRecordExternalIdType() {
    assertEquals(ExternalIdType.RECORD, QueryParamUtil.toExternalIdType("RECORD"));
  }

  @Test
  public void shouldReturnInstanceExternalIdType() {
    assertEquals(ExternalIdType.INSTANCE, QueryParamUtil.toExternalIdType("INSTANCE"));
  }

  @Test
  public void shouldReturnDefaultExternalIdType() {
    assertEquals(ExternalIdType.RECORD, QueryParamUtil.toExternalIdType(null));
    assertEquals(ExternalIdType.RECORD, QueryParamUtil.toExternalIdType(""));
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionForUnknownExternalIdType() {
    QueryParamUtil.toExternalIdType("UNKNOWN");
  }

  @Test
  public void shouldReturnMarcRecordType() {
    assertEquals(RecordType.MARC, QueryParamUtil.toRecordType("MARC"));
  }

  @Test
  public void shouldReturnEdifactRecordType() {
    assertEquals(RecordType.EDIFACT, QueryParamUtil.toRecordType("EDIFACT"));
  }

  @Test
  public void shouldReturnDefaultRecordType() {
    assertEquals(RecordType.MARC, QueryParamUtil.toRecordType(null));
    assertEquals(RecordType.MARC, QueryParamUtil.toRecordType(""));
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionForUnknownRecordType() {
    QueryParamUtil.toRecordType("UNKNOWN");
  }

}
