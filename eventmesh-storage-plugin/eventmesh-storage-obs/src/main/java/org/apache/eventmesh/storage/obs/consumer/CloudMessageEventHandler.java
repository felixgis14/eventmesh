package org.apache.eventmesh.storage.obs.consumer;

import com.alibaba.fastjson2.JSON;
import com.google.common.reflect.TypeToken;
import com.lmax.disruptor.EventHandler;
import com.obs.services.ObsClient;
import com.obs.services.model.GetObjectRequest;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.v1.CloudEventV1;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.api.EventListener;
import org.apache.eventmesh.api.EventMeshAction;
import org.apache.eventmesh.api.EventMeshAsyncConsumeContext;
import org.apache.eventmesh.storage.obs.broker.ObsBroker;
import org.apache.eventmesh.storage.obs.broker.ObsProperties;
import org.apache.eventmesh.storage.obs.cloudevent.CloudEventModel;
import org.apache.eventmesh.storage.obs.cloudevent.CloudMessageEvent;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

@Slf4j
public class CloudMessageEventHandler implements EventHandler<CloudMessageEvent> {

  @Override
  public void onEvent(CloudMessageEvent cloudMessageEvent, long sequence, boolean endOfBatch) {
    log.info("CloudMessageEventHandler Receive cloud message event======>{}", cloudMessageEvent);

    ObsBroker obsBroker = ObsBroker.getInstance();

    String topicName = cloudMessageEvent.getTopic();
    String msgId = cloudMessageEvent.getMsgId();
    ObsClient obsClient = obsBroker.getObsClient();
    ObsProperties obsProperties = obsBroker.getObsProperties();

    try {
      String objectKey = topicName + "/" + msgId + ".json";
      GetObjectRequest request = new GetObjectRequest(obsProperties.getBucket(), objectKey);
      log.info("CloudMessageEventHandler Get Obs object======>");
      ObsObject obsObject = obsClient.getObject(request);
      // 获取所有自定义元数据
      ObjectMetadata metadata = obsObject.getMetadata();
      InputStream objectContent = obsObject.getObjectContent();
      int available = metadata.getContentLength().intValue();
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      int nRead;
      byte[] data = new byte[available];
      while ((nRead = objectContent.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, nRead);
      }
      buffer.flush();
      byte[] cloudEvent = buffer.toByteArray();
      String cloudEventStr = new String(cloudEvent, StandardCharsets.UTF_8);
      log.info("CloudMessageEventHandler get cloudEvent======>{}", cloudEventStr);
      Type type = new TypeToken<CloudEventModel>() {
      }.getType();
      Object obj = JSON.parseObject(cloudEventStr, type);

      EventMeshAsyncConsumeContext consumeContext = new EventMeshAsyncConsumeContext() {
        @Override
        public void commit(EventMeshAction action) {
          switch (action) {
            case CommitMessage:
              // update offset
              log.info("CloudMessageEventHandler message commit, topic: {}, current messageId:{}", topicName, msgId);
              break;
            case ManualAck:
              // update offset
              log.info("CloudMessageEventHandler message ack, topic: {}, current offset:{}", topicName, msgId);
              break;
            case ReconsumeLater:
            default:
          }
        }
      };
      log.info("CloudMessageEventHandler EventListener will consume event======>");
      EventListener listener = obsBroker.getListener();
      listener.consume(obsBroker.toCloudEvent((CloudEventModel) obj), consumeContext);
      //删除obs存储的消息
      log.info("CloudMessageEventHandler Delete obs object:{}======>", objectKey);
      obsClient.deleteObject(obsProperties.getBucket(), objectKey);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
    }
  }
}
