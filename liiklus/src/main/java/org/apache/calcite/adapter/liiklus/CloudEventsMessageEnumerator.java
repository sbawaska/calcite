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
import io.cloudevents.CloudEvent;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;
import java.util.Map;

public class CloudEventsMessageEnumerator implements Enumerator<Object[]> {

  private LiiklusReader reader;
  private Map<String, SqlTypeName> additionalColumns;
  private Object[] current;
  private ObjectMapper mapper = new ObjectMapper();

  public CloudEventsMessageEnumerator(LiiklusReader reader, Map<String, SqlTypeName> additionalColumns) {
    this.reader = reader;
    this.additionalColumns = additionalColumns;
    this.current = new Object[4 + additionalColumns.size()];
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {

    try {
      CloudEvent event = reader.read();
      current[0] = event.getAttributes().getId();
      current[1] = event.getAttributes().getSource();
      current[2] = event.getAttributes().getSpecversion();
      current[3] = event.getAttributes().getType();
      String jsonString;
      if (event.getData().isPresent()) {
        jsonString= (String) event.getData().get();
      } else {
        jsonString = new String(event.getDataBase64());
      }
      Map<String, Object> data = mapper.readValue(jsonString, Map.class);
      int idx = 4;
      for (String k : this.additionalColumns.keySet()) {
        current[idx] = data.get(k);
        idx++;
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override public void reset() {

  }

  @Override public void close() {

  }
}
