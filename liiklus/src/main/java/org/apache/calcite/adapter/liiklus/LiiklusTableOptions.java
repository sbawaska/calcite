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

public final class LiiklusTableOptions {

  private String gatewayAddress;
  private String topicName;

  @Override public String toString() {
    return "LiiklusTableOptions{" +
      "gatewayAddress='" + gatewayAddress + '\'' +
      ", topicName='" + topicName + '\'' +
      '}';
  }

  public String getGatewayAddress() {
    return gatewayAddress;
  }

  public void setGatewayAddress(String gatewayAddress) {
    this.gatewayAddress = gatewayAddress;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public void validate() {
    if (this.gatewayAddress == null || this.topicName == null) {
      throw new IllegalArgumentException("Expected all options ");
    }
  }
}
