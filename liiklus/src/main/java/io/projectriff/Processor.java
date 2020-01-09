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

package io.projectriff;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Processor {
  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
    if (args.length != 1) {
      System.err.println("Expected only one argument");
      System.exit(1);
    }
    writeNewModel(args[0]);
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:calcite:model=new-liiklus.model.json");
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(args[0]);
    printHeader(resultSet);
    ResultSetMetaData rsmd = resultSet.getMetaData();
    while (resultSet.next()) {
      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        if (i > 1) System.out.print(" | ");
        System.out.print(resultSet.getString(i));
      }
      System.out.println();
    }
  }

  private static void printHeader(ResultSet resultSet) throws SQLException {
    ResultSetMetaData rsmd = resultSet.getMetaData();
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      if (i > 1) System.out.print(" | ");
      System.out.print(rsmd.getColumnName(i));
    }
    System.out.println();
    System.out.println("-----------------------------------------------------------------------------------------------");
  }

  private static void writeNewModel(String arg) throws IOException {
    String query = arg;
    String table = getTableName(query);
    ObjectMapper mapper = new ObjectMapper();
    Map model = mapper.readValue(new File("liiklus.model.json"), Map.class);
    List<Map> schemas = (List<Map>) model.get("schemas");
    List<Map> tables = (List<Map>) schemas.get(0).get("tables");
    Map t = tables.get(0);
    t.put("name", table.toUpperCase());
    Map operand = (Map) t.get("operand");
    operand.put("topic.name", "default_"+table);
    mapper.writeValue(new File("new-liiklus.model.json"), model);
  }

  private static String getTableName(String query) {
    String[] qWords = query.split("\\s+");
    for (int i=0; i <qWords.length; i++) {
      if (qWords[i].equalsIgnoreCase("from")) {
        return qWords[i+1].replace(';', ' ').trim();
      }
    }
    throw new IllegalArgumentException("invalid query:" + query);
  }
}
