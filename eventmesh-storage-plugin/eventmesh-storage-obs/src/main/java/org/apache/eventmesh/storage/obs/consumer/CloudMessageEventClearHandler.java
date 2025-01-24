package org.apache.eventmesh.storage.obs.consumer;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;

@Slf4j
public class CloudMessageEventClearHandler implements EventHandler<CloudMessageEvent> {

  @Override
  public void onEvent(CloudMessageEvent cloudMessageEvent, long l, boolean b) {
    log.info("Clear cloud message======>{}", cloudMessageEvent);
    cloudMessageEvent.clear();
  }
}
