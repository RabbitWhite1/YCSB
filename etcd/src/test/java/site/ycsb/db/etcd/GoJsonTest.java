/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.etcd;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Checks GoJson against vectors produced by Go's encoding/json (src/test/resources/go-json-vectors.*).
 */
public class GoJsonTest {

  private static Map<String, byte[]> input(String name) {
    Map<String, byte[]> m = new HashMap<>();
    byte[] hi = "hi".getBytes(StandardCharsets.UTF_8);
    switch (name) {
    case "record":
      for (int i = 0; i < 10; i++) {
        byte[] b = new byte[100];
        for (int j = 0; j < b.length; j++) {
          b[j] = (byte) ((i * 31 + j * 7) % 256);
        }
        m.put("field" + i, b);
      }
      break;
    case "keys":
      for (String k : new String[] {"b", "a", "é", "<k>&", "q\"\\", " x ", "tab\t\b\f\n\r",
          "\u0001\u001f\u007f", "Z", "𝄞"}) {
        m.put(k, hi);
      }
      break;
    case "empty":
      break;
    case "emptyval":
      m.put("x", new byte[0]);
      break;
    case "nilval":
      m.put("x", null);
      break;
    default:
      throw new IllegalArgumentException(name);
    }
    return m;
  }

  @Test
  public void matchesGoEncodingJson() throws Exception {
    int enc = 0;
    int dec = 0;
    try (BufferedReader r = new BufferedReader(new InputStreamReader(
        GoJsonTest.class.getResourceAsStream("/go-json-vectors.tsv"), StandardCharsets.UTF_8))) {
      String line;
      while ((line = r.readLine()) != null) {
        String[] f = line.split("\t");
        if (f[0].equals("ENC")) {
          assertArrayEquals(f[1], Base64.getDecoder().decode(f[2]), GoJson.encode(input(f[1])));
          enc++;
        } else {
          byte[] in = Base64.getDecoder().decode(f[2]);
          if (f[3].equals("ERROR")) {
            try {
              GoJson.decode(in);
              fail(f[1] + ": Go rejects this input");
            } catch (IllegalArgumentException expected) {
              // Go's decoder returns an error too.
            }
          } else {
            assertArrayEquals(f[1], Base64.getDecoder().decode(f[3]), GoJson.encode(GoJson.decode(in)));
          }
          dec++;
        }
      }
    }
    assertEquals(5, enc);
    assertEquals(10, dec);
  }

  @Test
  public void configLikeGoYcsb() {
    assertEquals(2_000_000_000L, EtcdClient.dialTimeoutNs(null));
    assertEquals(2_000_000_000L, EtcdClient.dialTimeoutNs("2s"));
    assertEquals(500L, EtcdClient.dialTimeoutNs("500"));
    URI[] u = EtcdClient.endpointUris("10.0.0.1:2379,http://10.0.0.2:2379", false);
    assertEquals("http://10.0.0.1:2379", u[0].toString());
    assertEquals("http://10.0.0.2:2379", u[1].toString());
    assertEquals("usertable:user1", new String(EtcdClient.rowKey("usertable", "user1").getBytes(),
        StandardCharsets.UTF_8));
  }
}
