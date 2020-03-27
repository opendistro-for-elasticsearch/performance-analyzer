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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.os;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchemaFileParser {
  private static final Logger LOGGER = LogManager.getLogger(SchemaFileParser.class);

  public enum FieldTypes {
    INT,
    STRING,
    CHAR,
    ULONG,
    DOUBLE;
  }

  private String filename = null;
  private String[] keys = null;
  private FieldTypes[] types = null;
  private boolean preProcess = false;

  public SchemaFileParser(String file, String[] keys, FieldTypes[] types) {
    this.filename = file;
    this.keys = keys.clone();
    this.types = types.clone();
  }

  // - from java 11 onwards, there is thread name in /proc/pid/task/tid/stat, which has spaces in it
  // - And threadname has "()" around it. Introduced a preprocess step to combine all of them
  public SchemaFileParser(String file, String[] keys, FieldTypes[] types, boolean preProcess) {
    this.filename = file;
    this.keys = keys.clone();
    this.types = types.clone();
    this.preProcess = preProcess;
  }

  private Object getTypedValue(String value, FieldTypes type) {
    switch (type) {
      case CHAR:
        return value.charAt(0);
      case INT:
        return Integer.valueOf(value);
      case STRING:
        return value;
      case ULONG:
        return Long.parseUnsignedLong(value);
      case DOUBLE:
        return Double.valueOf(value);
      default:
        return null;
    }
  }

  private void generateMap(String content, Map<String, Object> map) {
    String[] splitvalues = content.trim().split(" +");
    String[] values = preProcess(splitvalues);
    if (values.length < types.length) {
      LOGGER.debug(
          "Content Values tokens {} length is less than types {} length with ExceptionCode: {}",
          () -> Arrays.toString(values),
          () -> Arrays.toString(types),
          () -> StatExceptionCode.SCHEMA_PARSER_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.SCHEMA_PARSER_ERROR);
    }
    int lim = Math.min(values.length, types.length);
    for (int idx = 0; idx < lim; idx++) {
      map.put(keys[idx], getTypedValue(values[idx], types[idx]));
    }
  }

  private String[] preProcess(String[] tokens) {
    if (preProcess) {
      List<String> processedTokens = new ArrayList<>();
      StringBuffer tmp = new StringBuffer();
      boolean beingProcessed = false;
      for (int idx = 0; idx < tokens.length; idx++) {
        if (beingProcessed) {
          tmp.append(tokens[idx]);
          if (tokens[idx].endsWith(")")) {
            beingProcessed = false;
            processedTokens.add(tmp.toString());
            tmp.setLength(0);
          }
        } else if (tokens[idx].startsWith("(")) {
          if (tokens[idx].endsWith(")")) {
            processedTokens.add(tokens[idx]);
          } else {
            beingProcessed = true;
            tmp.append(tokens[idx]);
          }
        } else {
          processedTokens.add(tokens[idx]);
        }
      }
      return processedTokens.toArray(new String[processedTokens.size()]);
    } else {
      return tokens;
    }
  }

  /*
  to be used for parsing the outputs that contains single line
  */
  public Map<String, Object> parse() {
    Map<String, Object> map = new HashMap<>();
    try (FileReader fileReader = new FileReader(new File(filename));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line = bufferedReader.readLine();
      if (line == null) {
        return map;
      }
      generateMap(line, map);
    } catch (FileNotFoundException e) {
      LOGGER.debug("FileNotFound in parse with exception: {}", () -> e.toString());
    } catch (Exception e) {
      LOGGER.debug(
          "Error in parse with exception: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.SCHEMA_PARSER_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.SCHEMA_PARSER_ERROR);
    }
    return map;
  }

  /*
  to be used for parsing the outputs that contains multiple lines
  */
  public List<Map<String, Object>> parseMultiple() {
    List<Map<String, Object>> mapList = new ArrayList<>();
    try (FileReader fileReader = new FileReader(new File(filename));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        Map<String, Object> map = new HashMap<>();
        generateMap(line, map);
        mapList.add(map);
      }
    } catch (FileNotFoundException e) {
      LOGGER.debug("FileNotFound in parse with exception: {}", () -> e.toString());
    } catch (Exception e) {
      LOGGER.debug(
          "Error in parseMultiple with exception: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.SCHEMA_PARSER_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.SCHEMA_PARSER_ERROR);
    }
    return mapList;
  }
}
