package org.apache.eventmesh.storage.obs.cloudevent;

import lombok.Data;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Map;

@Data
public class CloudEventModel {

  private Map<String, Object> attributes;

  private byte[] data;

  private Map<String, Object> extensions;

  private String id;

  private URI source;

  private String specVersion;

  private String subject;

  private String type;

  private String dataContentType;

  private URI dataSchema;

  private OffsetDateTime time;
}
