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

package org.apache.eventmesh.storage.obs.admin;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.storage.obs.broker.ObsBroker;
import io.cloudevents.CloudEvent;
import org.apache.eventmesh.api.admin.AbstractAdmin;
import org.apache.eventmesh.api.admin.TopicProperties;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;
import org.apache.eventmesh.storage.obs.producer.CloudMessageEventProducer;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ObsAdmin extends AbstractAdmin {

  private final ObsBroker obsBroker;

  public ObsAdmin() {
    super(new AtomicBoolean(false));
    this.obsBroker = ObsBroker.getInstance();
  }

  @Override
  public List<TopicProperties> getTopic() {
    Multimap<String, Integer> topicMultimap = obsBroker.getTopicMultimap();
    if (topicMultimap == null || topicMultimap.isEmpty()) {
      return new ArrayList<>();
    }
    List<TopicProperties> topicPropertiesList = new ArrayList<>(topicMultimap.size());
    Iterator<Map.Entry<String, Collection<Integer>>> iterator = topicMultimap.asMap().entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Collection<Integer>> entry = iterator.next();
      TopicProperties topicProperties = new TopicProperties(entry.getKey(), entry.getValue().size());
      topicPropertiesList.add(topicProperties);
    }
    return topicPropertiesList;
  }

  @Override
  public void createTopic(String topicName) {
    log.info("Admin Creating topic {}", topicName);
    obsBroker.createTopicIfAbsent(topicName);
  }

  @Override
  public void deleteTopic(String topicName) {
    log.info("Admin delete topic {}", topicName);
    Multimap<String, Integer> topicMultimap = obsBroker.getTopicMultimap();
    if (topicMultimap != null && !topicMultimap.isEmpty()) {
      topicMultimap.removeAll(topicName);
    }
    obsBroker.deleteTopicIfExist(topicName);
  }


  @Override
  public void publish(CloudEvent cloudEvent) {
    log.info("Admin Publishing cloud event");
    Preconditions.checkNotNull(cloudEvent);
    try {
      String uuid = UUID.randomUUID().toString().replaceAll("-", "");
      obsBroker.putMessage(uuid, cloudEvent);
      //给Event填充数据
      Disruptor<CloudMessageEvent> disruptor = obsBroker.getDisruptor();
      RingBuffer<CloudMessageEvent> ringBuffer = disruptor.getRingBuffer();
      CloudMessageEventProducer producer = new CloudMessageEventProducer(ringBuffer);
      producer.onData(uuid, cloudEvent.getSubject());
    } catch (Exception e) {
      log.error("failed to add event to ringBuffer for : e = {},{}", e, e.getMessage());
    }
  }
}
