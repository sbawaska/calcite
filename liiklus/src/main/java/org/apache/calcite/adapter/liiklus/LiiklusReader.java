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
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LiiklusReader {

  private BlockingQueue<CloudEvent> q = new LinkedBlockingQueue<>();
  private BlockingQueue<CloudEvent> firstEvent = new SynchronousQueue<>();
  private AtomicBoolean firstSet = new AtomicBoolean(false);
  private LiiklusTableOptions tableOptions;
  private AtomicBoolean subscribed = new AtomicBoolean(false);

  public LiiklusReader(LiiklusTableOptions tableOptions) {
    this.tableOptions = tableOptions;
  }

  private void subscribe() {
    if (!subscribed.compareAndSet(false, true)) {
      return;
    }

    ManagedChannel channel = NettyChannelBuilder.forTarget(tableOptions.getGatewayAddress())
      .directExecutor()
      .usePlaintext()
      .build();

    LiiklusServiceGrpc.LiiklusServiceStub stub = LiiklusServiceGrpc.newStub(channel);

    SubscribeRequest subscribeAction = SubscribeRequest.newBuilder()
      .setTopic(tableOptions.getTopicName())
      .setGroup("my-group" + System.nanoTime())
      .setAutoOffsetReset(SubscribeRequest.AutoOffsetReset.EARLIEST)
      .build();

    final BlockingQueue<Assignment> subscribeAssignment = new SynchronousQueue<>();
    stub.subscribe(subscribeAction, new StreamObserver<SubscribeReply>() {
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
      }

      @Override
      public void onCompleted() {
      }
    });

    ReceiveRequest receiveRequest = null;
    try {
      receiveRequest = ReceiveRequest.newBuilder().setAssignment(subscribeAssignment.take()).build();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    stub.receive(receiveRequest, new StreamObserver<ReceiveReply>() {
      @Override
      public void onNext(ReceiveReply value) {
        try {
          String eventJson = new String(value.getRecord().getValue().toByteArray());
          CloudEvent event = Json.decodeValue(eventJson, new TypeReference<CloudEventImpl>() {});
          q.put(event);
          if (firstSet.compareAndSet(false, true)) {
            firstEvent.put(event);
          }
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


  public CloudEvent readFirst() throws InterruptedException {
    subscribe();
    return this.firstEvent.take();
  }

  public CloudEvent read() throws InterruptedException {
    return q.take();
  }

}
