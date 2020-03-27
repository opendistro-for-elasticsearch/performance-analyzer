/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.EventDispatcher;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a serializer - de-serializer class. This is used by the PerformancecAnalyzer Plugin to
 * write to serialize bytes before writing to file and by the PerformanceAnalyzer App to
 * de-serialize the bytes it has read from disk.
 */
public class EventLog {
  private int metricLocPathLength;
  private static final Logger LOG = LogManager.getLogger(EventLog.class);

  private static final char startMarker = '^';
  private static final char endMarker = '$';

  private static final char[] separator = System.lineSeparator().toCharArray();

  private Ret ret;

  public EventLog() {
    if (separator.length > 1) {
      throw new IllegalStateException("separator cannot be multi-byte");
    }
    // The path can be something like : /dev/shm/1566088110000/

    // This line takes the length of /dev/shm
    metricLocPathLength =
        Paths.get(PluginSettings.instance().getMetricsLocation()).toString().length();

    // This adds the length of the characters in the string form of current time in milliseconds.
    metricLocPathLength += String.valueOf(System.currentTimeMillis()).length();

    // This accounts for the '/' characters at the end of the /dev/shm and after the time.
    metricLocPathLength += 2;
  }

  public byte[] write(Event metric) {
    StringBuilder sb = new StringBuilder();
    sb.append(startMarker)
        .append(metric.key.substring(metricLocPathLength))
        .append(separator)
        .append(metric.value.toCharArray())
        .append(endMarker)
        .append(separator);
    return sb.toString().getBytes();
  }

  /**
   * This functions interprets the bytes and creates Event objects from it.
   *
   * <p>A non-corrupted byte stream should start with the startMarker. It may not end with the
   * endmarker based on how much of the actual bytes on the disk we are reading. This iterates
   * through the bytes and tries to interpret them into Event members. A complete Event object can
   * be something like this:
   *
   * <p>$heap_metrics {"current_time":1566110054768}
   * {"MemType":"totYoungGC","GC_Collection_Event":1, \ "GC_Collection_Time":6,
   * "Heap_Committed":-2,"Heap_Init":-2, \ "Heap_Max":-2,"Heap_Used":-2}
   * {"MemType":"totFullGC","GC_Collection_Event":0,"GC_Collection_Time":0, \
   * "Heap_Committed":-2,"Heap_Init":-2,"Heap_Max":-2,"Heap_Used":-2}
   * {"MemType":"PermGen","GC_Collection_Event":-2,"GC_Collection_Time":-2, \
   * "Heap_Committed":15335424,"Heap_Init":0,"Heap_Max":-1,"Heap_Used":14763104}
   * {"MemType":"Survivor","GC_Collection_Event":-2, \ "GC_Collection_Time":-2,
   * "Heap_Committed":11010048, \ "Heap_Init":11010048, "Heap_Max":11010048,"Heap_Used":6224432}
   * {"MemType":"OldGen","GC_Collection_Event":-2,"GC_Collection_Time":-2, \
   * "Heap_Committed":179306496,"Heap_Init":179306496, \ "Heap_Max":2863661056, "Heap_Used":16384}
   * {"MemType":"Eden","GC_Collection_Event":-2,"GC_Collection_Time":-2, \
   * "Heap_Committed":67108864,"Heap_Init":67108864,"Heap_Max":1409286144, \ "Heap_Used":30305704}
   * {"MemType":"NonHeap","GC_Collection_Event":-2,"GC_Collection_Time":-2, \
   * "Heap_Committed":21037056,"Heap_Init":2555904,"Heap_Max":-1, \ "Heap_Used":20207152}
   * {"MemType":"Heap","GC_Collection_Event":-2,"GC_Collection_Time":-2, \
   * "Heap_Committed":257425408,"Heap_Init":268435456, \ "Heap_Max":3817865216,
   * "Heap_Used":36546520} # $heap_metrics {"current_time":1566110055024} {"MemType":"totYoun
   *
   * <p>A Event object has three members: key, value and epoch. The serializer does not store the
   * epoch explicitly and its part of the data. In the above example: The bytes after '$' in the
   * line, is the key and all the other bytes leading up to '#', but not including it, is the value.
   * Sometimes the last few bytes in themselves cannot make a complete event object, so the return
   * also includes the count of unused bytes. The sender is supposed to send them again, in the next
   * iteration.
   *
   * <p>One corner case is, when the entirety of the bytes cannot create one Event object. This can
   * happen when the value is too large to fit in the buffer. In this case, the de-serialize sends
   * an empty list, The sender then has to read in more bytes and send it again. Sender essentially
   * has to increase the buffer size.
   *
   * @param byteBuffer The raw bytes in the file.
   */
  void read(final ByteBuffer byteBuffer, EventDispatcher processor) {
    if (ret == null) {
      ret = new Ret(new char[byteBuffer.limit()]);
    }

    while (byteBuffer.hasRemaining()) {
      char b = (char) byteBuffer.get();
      ret = processByte(b, ret, processor);
    }
  }

  static class Ret {
    String key;
    String value;
    int byteIdx;
    char[] bytes;

    Ret(char[] bytes) {
      this.key = "";
      this.value = "";
      this.byteIdx = 0;
      this.bytes = bytes;
    }
  }

  private static Ret processByte(char b, Ret arg, EventDispatcher processor) {
    if (b == separator[0] && arg.key.isEmpty()) {
      arg.key = new String(arg.bytes, 0, arg.byteIdx);
      // reset bytes
      arg.byteIdx = 0;
    } else if (b == endMarker) {
      // LOG.info("EndMarker found");
      arg.value = new String(arg.bytes, 0, arg.byteIdx);

      // Iterate through the key to figure out the index of the first file separator.
      int indexOfFirstPathSep = arg.key.indexOf(File.separatorChar);
      // TODO: Use to find the right parser
      String mapKey;
      if (indexOfFirstPathSep == -1) {
        mapKey = arg.key;
      } else {
        mapKey = arg.key.substring(0, indexOfFirstPathSep);
      }

      // The keys as in the files can be of the format a/b/c. The full
      // string goes inside the key of the Event but as part of the
      // map's key we only take the string up to the first file Separator.
      // So in this case, the key in the map will be just 'a'.
      Event event = new Event(arg.key, arg.value, 0);
      processor.processEvent(event);
    } else if (b == startMarker) {
      arg.key = "";
      // reset
      arg.byteIdx = 0;
    } else {
      arg.bytes[arg.byteIdx] = b;
      arg.byteIdx += 1;

      // If you run out of space, then grow the array and copy the data over.
      if (arg.byteIdx == arg.bytes.length) {
        // grow the bytebuffer
        char[] newBytes = new char[arg.bytes.length * 2];
        int i = 0;
        for (char c : arg.bytes) {
          newBytes[i] = c;
          i += 1;
        }
        arg.bytes = newBytes;
      }
    }
    return arg;
  }

  public void clear() {
    ret = null;
  }
}
