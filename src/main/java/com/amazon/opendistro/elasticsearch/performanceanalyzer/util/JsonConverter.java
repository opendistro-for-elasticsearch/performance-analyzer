/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonConverter {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Logger LOG = LogManager.getLogger(JsonConverter.class);

  /**
   * We can miss writing a metric if exception is thrown.
   *
   * @param value a Java object
   * @return the converted string from the input Java object
   */
  public static String writeValueAsString(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonGenerationException e) {
      LOG.warn("Json generation error " + e.getMessage());
      throw new IllegalArgumentException(e);
    } catch (JsonMappingException e) {
      LOG.warn("Json Mapping Error: " + e.getMessage());
      throw new IllegalArgumentException(e);
    } catch (IOException e) {
      LOG.warn("IO error: " + e.getMessage());
      throw new IllegalArgumentException(e);
    }
  }

  public static Map<String, Object> createMapFrom(String json) {

    try {
      if (json.trim().length() != 0) {
        return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {});
      }
    } catch (IOException e) {
      LOG.debug(
          "IO error: {} for json {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> json,
          () -> StatExceptionCode.JSON_PARSER_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.JSON_PARSER_ERROR);
    }
    return Collections.emptyMap();
  }

  /**
   * Search a Jackson JsonNode inside a JSON string matching the input path expression
   *
   * @param jsonString an encoded JSON string
   * @param paths path fragments
   * @return the matching Jackson JsonNode or null in case of no match.
   * @throws IOException if underlying input contains invalid content of type JsonParser supports
   * @throws JsonProcessingException if underlying input contains invalid content of type JsonParser
   *     supports
   * @throws IOException if underlying input contains invalid content of type JsonParser supports
   */
  public static JsonNode getChildNode(String jsonString, String... paths)
      throws JsonProcessingException, IOException {
    JsonNode rootNode = MAPPER.readTree(jsonString);
    return getChildNode(rootNode, paths);
  }

  /**
   * Search a Jackson JsonNode inside a Jackson JsonNode matching the input path expression
   *
   * @param jsonNode a Jackson JsonNode
   * @param paths path fragments
   * @return the matching Jackson JsonNode or null in case of no match.
   */
  public static JsonNode getChildNode(JsonNode jsonNode, String... paths) {
    for (int i = 0; i < paths.length; i++) {
      String path = paths[i];
      if (!jsonNode.has(path)) {
        return null;
      }

      jsonNode = jsonNode.get(path);
    }

    return jsonNode;
  }

  /**
   * Search a long number inside a JSON string matching the input path expression
   *
   * @param jsonString an encoded JSON string
   * @param paths path fragments
   * @return the matching long number or null in case of no match.
   * @throws JsonPathNotFoundException thrown if the input path is invalid
   * @throws IOException thrown if underlying input contains invalid content of type JsonParser
   *     supports
   * @throws JsonProcessingException thrown if underlying input contains invalid content of type
   *     JsonParser supports
   */
  public static long getLongValue(String jsonString, String... paths)
      throws JsonPathNotFoundException, JsonProcessingException, IOException {
    JsonNode jsonNode = getChildNode(jsonString, paths);
    if (jsonNode != null) {
      return jsonNode.longValue();
    }
    throw new JsonPathNotFoundException();
  }
}
