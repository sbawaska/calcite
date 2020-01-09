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

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.bsideup.liiklus.protocol.*;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;

import io.cloudevents.v1.CloudEventImpl;
import io.grpc.stub.StreamObserver;
import org.apache.calcite.linq4j.Enumerator;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloudEventsMessageEnumerator implements Enumerator<Object[]> {

  private LiiklusServiceGrpc.LiiklusServiceStub liiklusServiceStub;
  private LiiklusTableOptions tableOptions;
  private BlockingQueue<CloudEvent> q = new LinkedBlockingQueue<>();
  private AtomicBoolean subscribed = new AtomicBoolean(false);
  private Object[] current = new Object[5];

  public CloudEventsMessageEnumerator(LiiklusTableOptions tableOptions, LiiklusServiceGrpc.LiiklusServiceStub stub) {
    this.liiklusServiceStub = stub;
    this.tableOptions = tableOptions;

  }

  private void subscribe() {
    if (!subscribed.compareAndSet(false, true)) {
      return;
    }
    SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
      .setTopic(tableOptions.getTopicName())
      .setGroup("my-group" + System.nanoTime())
      .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
      .build();

    final BlockingQueue<Assignment> subscribeAssignment = new SynchronousQueue<>();
    this.liiklusServiceStub.subscribe(subscribeAction, new StreamObserver<SubscribeReply>() {
      @Override
      public void onNext(SubscribeReply value) {
        try {
          subscribeAssignment.put(value.getAssignment());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void onError(Throwable t) {
        close();
      }

      @Override
      public void onCompleted() {
        close();
      }
    });

    ReceiveRequest receiveRequest = null;
    try {
      receiveRequest = ReceiveRequest.newBuilder().setAssignment(subscribeAssignment.take()).build();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    this.liiklusServiceStub.receive(receiveRequest, new StreamObserver<ReceiveReply>() {
      @Override
      public void onNext(ReceiveReply value) {
        try {
          String eventJson = new String(value.getRecord().getValue().toByteArray());
          CloudEvent event = Json.decodeValue(eventJson, new TypeReference<CloudEventImpl>() {});
          q.put(event);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void onError(Throwable t) {
        t.printStackTrace();
      }

      @Override
      public void onCompleted() {
      }
    });
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {
    subscribe();
    try {
      CloudEvent event = q.take();
      current[0] = event.getAttributes().getId();
      current[1] = event.getAttributes().getSource();
      current[2] = event.getAttributes().getSpecversion();
      current[3] = event.getAttributes().getType();
      if (event.getData().isPresent()) {
        current[4] = event.getData().get();
      } else {
        current[4] = new String(event.getDataBase64());
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return false;
  }

  @Override public void reset() {

  }

  @Override public void close() {

  }
}
