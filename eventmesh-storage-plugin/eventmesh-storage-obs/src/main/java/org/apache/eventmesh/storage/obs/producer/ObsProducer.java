/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.storage.obs.producer;

import com.google.common.base.Preconditions;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.eventmesh.api.producer.Producer;
import org.apache.eventmesh.storage.obs.broker.ObsBroker;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.RequestReplyCallback;
import org.apache.eventmesh.api.SendCallback;
import org.apache.eventmesh.api.SendResult;
import org.apache.eventmesh.api.exception.OnExceptionContext;
import org.apache.eventmesh.api.exception.StorageRuntimeException;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ObsProducer implements Producer {

  private ObsBroker obsBroker;

  private Disruptor<CloudMessageEvent> disruptor;

  private AtomicBoolean isStarted;

  public ObsProducer() {
    this.obsBroker = ObsBroker.getInstance();
    this.isStarted = new AtomicBoolean(false);
  }

  public boolean isStarted() {
    return isStarted.get();
  }

  public boolean isClosed() {
    return !isStarted.get();
  }

  public void start() {
    isStarted.compareAndSet(false, true);
    disruptor.start();
  }

  public void shutdown() {
    isStarted.compareAndSet(true, false);
    disruptor.shutdown();
  }

  public void init(Properties properties) {
    log.info("ObsProducer init======>");
    this.disruptor = obsBroker.getDisruptor();
  }

  public void publish(CloudEvent cloudEvent, SendCallback sendCallback) {
    log.info("ObsProducer publish======>");
    Preconditions.checkNotNull(cloudEvent);
    Preconditions.checkNotNull(sendCallback);

    try {
      SendResult sendResult = publish(cloudEvent);
      sendCallback.onSuccess(sendResult);
    } catch (Exception ex) {
      OnExceptionContext onExceptionContext = OnExceptionContext.builder()
          .messageId(cloudEvent.getId())
          .topic(cloudEvent.getSubject())
          .exception(new StorageRuntimeException(ex))
          .build();
      sendCallback.onException(onExceptionContext);
    }
  }

  public void sendOneway(CloudEvent cloudEvent) {
    publish(cloudEvent);
  }

  public SendResult publish(CloudEvent cloudEvent) {
    Preconditions.checkNotNull(cloudEvent);
    SendResult sendResult = new SendResult();
    try {
      String uuid = UUID.randomUUID().toString().replaceAll("-", "");
      log.info("ObsProducer save cloudevent to OBS======>");
      obsBroker.putMessage(uuid, cloudEvent);

      RingBuffer<CloudMessageEvent> ringBuffer = disruptor.getRingBuffer();
      CloudMessageEventProducer producer = new CloudMessageEventProducer(ringBuffer);
      producer.onData(uuid, cloudEvent.getSubject());

      sendResult.setTopic(cloudEvent.getSubject());
      sendResult.setMessageId(uuid);
    } catch (Exception e) {
      log.error("failed to add event to ringBuffer for : e = {},{}", e, e.getMessage());
    }
    return sendResult;
  }

  public void request(CloudEvent cloudEvent, RequestReplyCallback rrCallback, long timeout) throws Exception {
    throw new StorageRuntimeException("Request is not supported");
  }

  public boolean reply(CloudEvent cloudEvent, SendCallback sendCallback) throws Exception {
    throw new StorageRuntimeException("Reply is not supported");
  }

  public void checkTopicExist(String topic) {
    boolean exist = obsBroker.checkTopicNotExist(topic);
    if (exist) {
      throw new StorageRuntimeException(String.format("topic:%s is not exist", topic));
    }
  }

  public void setExtFields() {

  }
}
