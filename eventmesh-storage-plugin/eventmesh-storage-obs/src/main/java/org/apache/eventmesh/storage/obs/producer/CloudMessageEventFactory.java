package org.apache.eventmesh.storage.obs.producer;

import com.lmax.disruptor.EventFactory;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;

public class CloudMessageEventFactory implements EventFactory<CloudMessageEvent> {

  @Override
  public CloudMessageEvent newInstance() {
    return new CloudMessageEvent();
  }
}
