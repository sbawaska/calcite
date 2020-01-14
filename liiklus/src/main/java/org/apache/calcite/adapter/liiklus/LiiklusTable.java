/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.liiklus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.bsideup.liiklus.protocol.LiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.common.collect.ImmutableList;
import io.cloudevents.CloudEvent;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class LiiklusTable implements ScannableTable, StreamableTable {

  private LiiklusTableOptions tableOptions;
  private LiiklusReader reader;
  private ObjectMapper mapper = new ObjectMapper();
  private Map<String, SqlTypeName> additionalColumns;

  public LiiklusTable(LiiklusTableOptions tableOptions) {
    this.tableOptions = tableOptions;
    this.reader = new LiiklusReader(tableOptions);
  }

  @Override public Table stream() {
    return this;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    Map<String, SqlTypeName> additionalColumns = getDataColumns();
    final RelDataType mapType = typeFactory.createMapType(
      typeFactory.createSqlType(SqlTypeName.VARCHAR),
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    fieldInfo.add("CE_ID", SqlTypeName.VARCHAR);
    fieldInfo.add("SOURCE", SqlTypeName.VARCHAR);
    fieldInfo.add("SPEC_VERSION", SqlTypeName.VARCHAR);
    fieldInfo.add("TYPE", SqlTypeName.VARCHAR);
    for (String colName : additionalColumns.keySet()) {
      fieldInfo.add(colName.toUpperCase(), additionalColumns.get(colName));
    }
    return fieldInfo.build();
  }

  private Map<String, SqlTypeName> getDataColumns() {
    if (this.additionalColumns != null) {
      return this.additionalColumns;
    }
    Map<String, SqlTypeName> retVal = new LinkedHashMap<>();
    try {
      CloudEvent event = this.reader.readFirst();
      String jsonString;
      if (event.getData().isPresent()) {
        jsonString = (String) event.getData().get();
      } else {
        jsonString = new String(event.getDataBase64());
      }
      Map<String, Object> data = mapper.readValue(jsonString, Map.class);
      for (String k : data.keySet()) {
        Object v = data.get(k);
        SqlTypeName sqlType;
        if (v instanceof Integer) {
          sqlType = SqlTypeName.INTEGER;
        } else {
          sqlType = SqlTypeName.VARCHAR;
        }
        retVal.put(k, sqlType);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    this.additionalColumns = retVal;
    return this.additionalColumns;
  }

  @Override public Statistic getStatistic() {
    return Statistics.of(100d, ImmutableList.of(),
      RelCollations.createSingleton(0));
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.STREAM;
  }

  @Override public boolean isRolledUp(String column) {
    return false;
  }

  @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return false;
  }

  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {

        return new CloudEventsMessageEnumerator(reader, additionalColumns);
      }
    };
  }
}
