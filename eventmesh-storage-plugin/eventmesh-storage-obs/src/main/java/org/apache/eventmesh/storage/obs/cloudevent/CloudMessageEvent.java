package org.apache.eventmesh.storage.obs.cloudevent;

import lombok.Data;

@Data
public class CloudMessageEvent {

  private String msgId;

  private String topic;

  public void clear() {
    msgId = null;
    topic = null;
  }
}
