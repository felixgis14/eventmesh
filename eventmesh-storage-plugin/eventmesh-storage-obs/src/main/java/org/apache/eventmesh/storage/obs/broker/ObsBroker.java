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

package org.apache.eventmesh.storage.obs.broker;

import com.alibaba.fastjson2.JSON;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AccessControlList;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import com.obs.services.model.PutObjectRequest;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.common.Constants;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.storage.obs.cloudevent.CloudEventModel;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;
import org.apache.eventmesh.storage.obs.consumer.CloudMessageEventClearHandler;
import org.apache.eventmesh.storage.obs.consumer.CloudMessageEventHandler;
import org.apache.eventmesh.storage.obs.producer.CloudMessageEventFactory;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.eventmesh.common.Constants.CLOUD_EVENTS_PROTOCOL_NAME;

@Slf4j
public class ObsBroker {

  @Getter
  private ObsClient obsClient;

  @Getter
  private ObsProperties obsProperties;

  @Getter
  private Multimap<String, Integer> topicMultimap = ArrayListMultimap.create();

  @Getter
  @Setter
  private EventListener listener;

  private Disruptor<CloudMessageEvent> disruptor;

  private ObsBroker() {
    ConfigService configService = ConfigService.getInstance();
    this.obsProperties = configService.buildConfigInstance(ObsProperties.class);
    this.obsClient = new ObsClient(obsProperties.getAccessKey(), obsProperties.getSecret(), obsProperties.getEndpoint());
    this.disruptor = createDisruptor();
  }


  public static ObsBroker getInstance() {
    return ObsBrokerInstanceHolder.INSTANCE;
  }

  /**
   * put message
   *
   * @param messageId topic name
   * @param message   message
   */
  public void putMessage(String messageId, CloudEvent message) {
    log.info("CloudEvent message class:{}", message.getClass().getName());
    String topicName = message.getSubject();
    createTopic(topicName);
    topicMultimap.put(topicName, 1);
    //保存消息到obs
    try {
      PutObjectRequest putObjectRequest = new PutObjectRequest();
      putObjectRequest.setBucketName(obsProperties.getBucket());
      putObjectRequest.setObjectKey(topicName + "/" + messageId + ".json");
      putObjectRequest.setInput(new ByteArrayInputStream(JSON.toJSONBytes(toCloudEventModel(message), StandardCharsets.UTF_8)));
      putObjectRequest.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
      obsClient.putObject(putObjectRequest);
    } catch (ObsException e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * topicName不存在就创建新目录
   *
   * @param topicName
   */
  public void createTopic(String topicName) {
    if (checkTopicNotExist(topicName)) {
      try {
        PutObjectRequest putObjectRequest = new PutObjectRequest();
        putObjectRequest.setBucketName(obsProperties.getBucket());
        putObjectRequest.setObjectKey(topicName + "/");
        putObjectRequest.setInput(new ByteArrayInputStream(new byte[0]));
        putObjectRequest.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
        obsClient.putObject(putObjectRequest);
      } catch (ObsException e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  /**
   * Get the message, if the queue is empty then await
   *
   * @param topicName
   */
  public CloudEvent takeMessage(String topicName) throws InterruptedException {
    return null;
  }

  /**
   * Get the message, if the queue is empty return null
   *
   * @param topicName
   */
  public CloudEvent getMessage(String topicName) {
    return null;
  }

  /**
   * Get the message by offset
   *
   * @param topicName topic name
   * @param offset    offset
   * @return CloudEvent
   */
  public CloudEvent getMessage(String topicName, long offset) {
    return null;
  }


  public boolean checkTopicNotExist(String topicName) {
    ListObjectsRequest request = new ListObjectsRequest(obsProperties.getBucket());
    request.setMaxKeys(1);
    request.setPrefix(topicName);
    ObjectListing result = obsClient.listObjects(request);
    return result.getObjects().isEmpty();
  }

  /**
   * if the topic does not exist, create the topic
   *
   * @param topicName topicName
   * @return Channel
   */
  public void createTopicIfAbsent(String topicName) {
    createTopic(topicName);
  }

  /**
   * if the topic exists, delete the topic
   *
   * @param topicName topicName
   */
  public void deleteTopicIfExist(String topicName) {
    //删除topicName下的所有文件
    try {
      String objectPrefix = topicName + "/";
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest(obsProperties.getBucket());
      listObjectsRequest.setMaxKeys(100);
      listObjectsRequest.setPrefix(objectPrefix);
      ObjectListing objectListing;
      do {
        objectListing = obsClient.listObjects(listObjectsRequest);
        DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(obsProperties.getBucket());
        deleteRequest.setQuiet(true);
        for (ObsObject object : objectListing.getObjects()) {
          deleteRequest.addKeyAndVersion(object.getObjectKey(), null);
        }
        if (deleteRequest.getKeyAndVersions().length > 0) {
          DeleteObjectsResult deleteResult = obsClient.deleteObjects(deleteRequest);
          log.info("DeletedObjectResults:" + deleteResult.getDeletedObjectResults());
          log.info("ErrorResults:" + deleteResult.getErrorResults());
        }
        listObjectsRequest.setMarker(objectListing.getNextMarker());
      } while (objectListing.isTruncated());
      log.info("deleteObjects successfully");
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }

  public Disruptor<CloudMessageEvent> getDisruptor() {
    return this.disruptor;
  }

  private Disruptor<CloudMessageEvent> createDisruptor() {
    CloudMessageEventFactory cloudMessageEventFactory = new CloudMessageEventFactory();
    int bufferSize = 1024 * 1024;
    //实例化 Disruptor
    Disruptor<CloudMessageEvent> cloudMessageEventDisruptor = new Disruptor<>(
        cloudMessageEventFactory,
        bufferSize,
        new CloudMessageEventThreadFactory(),
        // 单生产者
        ProducerType.SINGLE,
        // 阻塞等待策略
        new BlockingWaitStrategy());
    cloudMessageEventDisruptor.handleEventsWith(new CloudMessageEventHandler());
    cloudMessageEventDisruptor.setDefaultExceptionHandler(new IgnoreExceptionHandler());
    return cloudMessageEventDisruptor;
  }

  private static class ObsBrokerInstanceHolder {
    private static final ObsBroker INSTANCE = new ObsBroker();
  }

  private CloudEventModel toCloudEventModel(CloudEvent cloudEvent) {
    CloudEventModel cloudEventModel = new CloudEventModel();
    cloudEventModel.setId(cloudEvent.getId());
    cloudEventModel.setSubject(cloudEvent.getSubject());
    cloudEventModel.setData(cloudEvent.getData().toBytes());
    cloudEventModel.setSource(cloudEvent.getSource());
    cloudEventModel.setType(cloudEvent.getType());
    Map<String, Object> extensions = new HashMap<>();
    if (!CollectionUtils.isEmpty(cloudEvent.getExtensionNames())) {
      for (String entry : cloudEvent.getExtensionNames()) {
        extensions.put(entry, cloudEvent.getExtension(entry) == null ? "" : cloudEvent.getExtension(entry));
      }
    }
    cloudEventModel.setExtensions(extensions);
    Map<String, Object> attributes = new HashMap<>();
    if (!CollectionUtils.isEmpty(cloudEvent.getAttributeNames())) {
      for (String entry : cloudEvent.getAttributeNames()) {
        attributes.put(entry, cloudEvent.getAttribute(entry) == null ? "" : cloudEvent.getAttribute(entry));
      }
    }
    cloudEventModel.setAttributes(attributes);
    cloudEventModel.setTime(cloudEvent.getTime());
    cloudEventModel.setDataSchema(cloudEvent.getDataSchema());
    cloudEventModel.setDataContentType(cloudEvent.getDataContentType());
    return cloudEventModel;
  }

  public CloudEvent toCloudEvent(CloudEventModel cloudEventModel) {
    io.cloudevents.core.v1.CloudEventBuilder cloudEventBuilder = CloudEventBuilder.v1()
        .withId(UUID.randomUUID().toString())
        .withSubject(cloudEventModel.getSubject())
        .withSource(cloudEventModel.getSource())
        .withDataContentType(cloudEventModel.getDataContentType())
        .withType(cloudEventModel.getType())
        .withTime(cloudEventModel.getTime())
        .withData(cloudEventModel.getData());
    if (!ObjectUtils.isEmpty(cloudEventModel.getExtensions())) {
      Iterator<Map.Entry<String, Object>> iterator = cloudEventModel.getExtensions().entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Object> entry = iterator.next();
        Object value = entry.getValue();
        if (value instanceof Integer) {
          cloudEventBuilder.withExtension(entry.getKey(), (Integer) value);
        } else if (value instanceof Number) {
          cloudEventBuilder.withExtension(entry.getKey(), (Number) value);
        } else if (value instanceof Boolean) {
          cloudEventBuilder.withExtension(entry.getKey(), (Boolean) value);
        } else if (value instanceof OffsetDateTime) {
          cloudEventBuilder.withExtension(entry.getKey(), (OffsetDateTime) value);
        } else if (value instanceof URI) {
          cloudEventBuilder.withExtension(entry.getKey(), (URI) value);
        } else {
          cloudEventBuilder.withExtension(entry.getKey(), (String) value);
        }
      }
    }
    return cloudEventBuilder.build();
  }

  static class CloudMessageEventThreadFactory implements ThreadFactory {

    final AtomicInteger threadNum = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "CloudMessageEvent" + "-" + threadNum.incrementAndGet());
    }
  }
}