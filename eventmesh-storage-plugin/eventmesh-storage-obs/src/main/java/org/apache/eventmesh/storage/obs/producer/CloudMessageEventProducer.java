package org.apache.eventmesh.storage.obs.producer;

import com.lmax.disruptor.RingBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;

@Slf4j
public class CloudMessageEventProducer {

  private final RingBuffer<CloudMessageEvent> ringBuffer;

  public CloudMessageEventProducer(RingBuffer<CloudMessageEvent> ringBuffer) {
    this.ringBuffer = ringBuffer;
  }

  public void onData(String uuid, String topicName) {
    long sequence = ringBuffer.next();
    try {
      CloudMessageEvent cloudMessageEvent = ringBuffer.get(sequence);
      cloudMessageEvent.setMsgId(uuid);
      cloudMessageEvent.setTopic(topicName);
    } finally {
      log.info("CloudMessageEventProducer publish======>");
      ringBuffer.publish(sequence);
    }
  }
}
