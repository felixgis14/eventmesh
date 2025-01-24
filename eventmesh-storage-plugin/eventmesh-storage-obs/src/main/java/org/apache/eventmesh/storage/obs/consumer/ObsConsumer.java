package org.apache.eventmesh.storage.obs.consumer;

import com.google.common.collect.Multimap;
import io.cloudevents.CloudEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.AbstractContext;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.consumer.Consumer;
import org.apache.eventmesh.storage.obs.broker.ObsBroker;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ObsConsumer implements Consumer {

  private ObsBroker obsBroker;

  private AtomicBoolean isStarted;

  public ObsConsumer() {
    this.obsBroker = ObsBroker.getInstance();
    this.isStarted = new AtomicBoolean(false);
  }

  @Override
  public void init(Properties keyValue) throws Exception {
    log.info("ObsConsumer init======>");
  }

  @Override
  public void updateOffset(List<CloudEvent> cloudEvents, AbstractContext context) {

  }

  @Override
  public void subscribe(String topic) {

  }

  @Override
  public void unsubscribe(String topicName) {
    obsBroker.deleteTopicIfExist(topicName);
    Multimap<String, Integer> topicMultimap = obsBroker.getTopicMultimap();
    if (topicMultimap != null && !topicMultimap.isEmpty()) {
      topicMultimap.removeAll(topicName);
    }
  }

  @Override
  public void registerEventListener(EventListener listener) {
    log.info("ObsConsumer registerEventListener======>");
    obsBroker.setListener(listener);
  }

  @Override
  public boolean isStarted() {
    return isStarted.get();
  }

  @Override
  public boolean isClosed() {
    return !isStarted.get();
  }

  @Override
  public void start() {
    log.info("ObsConsumer start======>");
    isStarted.compareAndSet(false, true);
  }

  @Override
  public void shutdown() {
    isStarted.compareAndSet(true, false);
  }
}
