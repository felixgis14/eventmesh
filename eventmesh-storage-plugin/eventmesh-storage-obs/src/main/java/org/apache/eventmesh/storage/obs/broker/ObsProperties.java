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

import lombok.Data;
import org.apache.eventmesh.common.config.Config;
import org.apache.eventmesh.common.config.ConfigField;

@Data
@Config(prefix = "eventMesh.server.obs", path = "classPath://obs-client.properties")
public class ObsProperties {

  @ConfigField(field = "endpoint")
  private String endpoint;

  @ConfigField(field = "projectId")
  private String projectId;

  @ConfigField(field = "regin")
  private String regin;

  @ConfigField(field = "bucket")
  private String bucket;

  @ConfigField(field = "accessKey")
  private String accessKey;

  @ConfigField(field = "secret")
  private String secret;
}
