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

import com.github.bsideup.liiklus.protocol.LiiklusServiceGrpc;
import com.github.bsideup.liiklus.protocol.SubscribeRequest;
import com.google.common.collect.ImmutableList;
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


public class LiiklusTable implements ScannableTable, StreamableTable {

  private LiiklusTableOptions tableOptions;

  public LiiklusTable(LiiklusTableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }

  @Override public Table stream() {
    return this;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final RelDataType mapType = typeFactory.createMapType(
      typeFactory.createSqlType(SqlTypeName.VARCHAR),
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    fieldInfo.add("ID", SqlTypeName.VARCHAR);
    fieldInfo.add("SOURCE", SqlTypeName.VARCHAR);
    fieldInfo.add("SPEC_VERSION", SqlTypeName.VARCHAR);
    fieldInfo.add("TYPE", SqlTypeName.VARCHAR);
    fieldInfo.add("DATA", mapType);
    return fieldInfo.build();
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

        ManagedChannel channel = NettyChannelBuilder.forTarget(tableOptions.getGatewayAddress())
          .directExecutor()
          .usePlaintext()
          .build();

        LiiklusServiceGrpc.LiiklusServiceStub stub = LiiklusServiceGrpc.newStub(channel);

        return new CloudEventsMessageEnumerator(tableOptions, stub);
      }
    };
  }
}
