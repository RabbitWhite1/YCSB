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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Go's {@code encoding/json} for {@code map[string][]byte}, the record format of go-ycsb's etcd binding.
 *
 * <p>{@link #encode} produces the same bytes as Go's {@code json.Marshal}: keys sorted by their UTF-8
 * bytes, each value a standard base64 string (with padding), no whitespace, and Go's string escaping
 * (HTML-safe, with {@code \b \f \n \r \t} short forms). {@link #decode} accepts what Go's
 * {@code json.Decoder} accepts for that type: one JSON object whose values are base64 strings (or null),
 * the last of duplicate keys winning, and anything after the first value ignored.
 */
public final class GoJson {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private GoJson() {
  }

  /** The JSON of {@code values}, as Go's {@code json.Marshal(map[string][]byte)} writes it. */
  public static byte[] encode(Map<String, byte[]> values) {
    byte[][] keys = new byte[values.size()][];
    byte[][] vals = new byte[values.size()][];
    int n = 0;
    for (Map.Entry<String, byte[]> e : values.entrySet()) {
      keys[n] = e.getKey().getBytes(StandardCharsets.UTF_8);
      vals[n] = e.getValue();
      n++;
    }
    Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> compareBytes(keys[a], keys[b]));
    StringBuilder sb = new StringBuilder(16 + values.size() * 160);
    sb.append('{');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      appendString(sb, keys[order[i]]);
      sb.append(':');
      byte[] v = vals[order[i]];
      if (v == null) {
        sb.append("null");
      } else {
        sb.append('"').append(Base64.getEncoder().encodeToString(v)).append('"');
      }
    }
    sb.append('}');
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Decodes a Go-encoded {@code map[string][]byte}.
   *
   * @throws IllegalArgumentException where Go's decoder returns an error
   */
  public static Map<String, byte[]> decode(byte[] data) {
    Parser p = new Parser(new String(data, StandardCharsets.UTF_8));
    p.skipSpace();
    if (p.peek() != '{') {
      throw new IllegalArgumentException("not a JSON object");
    }
    p.pos++;
    Map<String, byte[]> result = new LinkedHashMap<>();
    p.skipSpace();
    if (p.peek() == '}') {
      p.pos++;
      return result;
    }
    while (true) {
      p.skipSpace();
      String key = p.readString();
      p.skipSpace();
      p.expect(':');
      p.skipSpace();
      if (p.peek() == 'n') {
        p.expectWord("null");
        result.put(key, null);
      } else {
        result.put(key, decodeBase64(p.readString()));
      }
      p.skipSpace();
      char c = p.next();
      if (c == '}') {
        return result;
      }
      if (c != ',') {
        throw new IllegalArgumentException("expected ',' or '}' at " + (p.pos - 1));
      }
    }
  }

  /** Go decodes []byte with base64.StdEncoding, which skips '\r' and '\n'. */
  private static byte[] decodeBase64(String s) {
    if (s.indexOf('\r') >= 0 || s.indexOf('\n') >= 0) {
      s = s.replace("\r", "").replace("\n", "");
    }
    // StdEncoding requires padding; Java's decoder would accept a missing one.
    if (s.length() % 4 != 0) {
      throw new IllegalArgumentException("illegal base64 data: missing padding");
    }
    return Base64.getDecoder().decode(s);
  }

  private static int compareBytes(byte[] a, byte[] b) {
    int n = Math.min(a.length, b.length);
    for (int i = 0; i < n; i++) {
      int d = (a[i] & 0xff) - (b[i] & 0xff);
      if (d != 0) {
        return d;
      }
    }
    return a.length - b.length;
  }

  /** Go's encodeState.string with escapeHTML: invalid UTF-8 becomes U+FFFD, U+2028/9 are escaped. */
  private static void appendString(StringBuilder sb, byte[] s) {
    sb.append('"');
    int i = 0;
    while (i < s.length) {
      int b = s[i] & 0xff;
      if (b < 0x80) {
        appendAscii(sb, b);
        i++;
        continue;
      }
      int len = utf8Length(s, i);
      if (len == 0) {
        sb.append("\\ufffd");
        i++;
        continue;
      }
      String rune = new String(s, i, len, StandardCharsets.UTF_8);
      int cp = rune.codePointAt(0);
      if (cp == 0x2028 || cp == 0x2029) {
        sb.append("\\u202").append(HEX[cp & 0xf]);
      } else {
        sb.append(rune);
      }
      i += len;
    }
    sb.append('"');
  }

  private static void appendAscii(StringBuilder sb, int b) {
    switch (b) {
    case '\\':
      sb.append("\\\\");
      break;
    case '"':
      sb.append("\\\"");
      break;
    case '\b':
      sb.append("\\b");
      break;
    case '\f':
      sb.append("\\f");
      break;
    case '\n':
      sb.append("\\n");
      break;
    case '\r':
      sb.append("\\r");
      break;
    case '\t':
      sb.append("\\t");
      break;
    case '<':
    case '>':
    case '&':
      sb.append("\\u00").append(HEX[b >> 4]).append(HEX[b & 0xf]);
      break;
    default:
      if (b < 0x20) {
        sb.append("\\u00").append(HEX[b >> 4]).append(HEX[b & 0xf]);
      } else {
        sb.append((char) b);
      }
      break;
    }
  }

  /** Length of the valid UTF-8 sequence at s[i] (as Go's utf8.DecodeRune accepts it), or 0 if invalid. */
  private static int utf8Length(byte[] s, int i) {
    int b0 = s[i] & 0xff;
    int len;
    int min;
    if (b0 >= 0xc2 && b0 <= 0xdf) {
      len = 2;
      min = 0x80;
    } else if (b0 >= 0xe0 && b0 <= 0xef) {
      len = 3;
      min = 0x800;
    } else if (b0 >= 0xf0 && b0 <= 0xf4) {
      len = 4;
      min = 0x10000;
    } else {
      return 0;
    }
    if (i + len > s.length) {
      return 0;
    }
    int cp = b0 & (0xff >> (len + 1));
    for (int k = 1; k < len; k++) {
      int b = s[i + k] & 0xff;
      if ((b & 0xc0) != 0x80) {
        return 0;
      }
      cp = (cp << 6) | (b & 0x3f);
    }
    if (cp < min || cp > 0x10ffff || (cp >= 0xd800 && cp <= 0xdfff)) {
      return 0;
    }
    return len;
  }

  /** A minimal JSON reader for the object/string/null subset decode needs. */
  private static final class Parser {
    private final String s;
    private int pos;

    Parser(String s) {
      this.s = s;
    }

    char peek() {
      if (pos >= s.length()) {
        throw new IllegalArgumentException("unexpected end of JSON input");
      }
      return s.charAt(pos);
    }

    char next() {
      char c = peek();
      pos++;
      return c;
    }

    void skipSpace() {
      while (pos < s.length()) {
        char c = s.charAt(pos);
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
          return;
        }
        pos++;
      }
    }

    void expect(char c) {
      if (next() != c) {
        throw new IllegalArgumentException("expected '" + c + "' at " + (pos - 1));
      }
    }

    void expectWord(String w) {
      if (!s.startsWith(w, pos)) {
        throw new IllegalArgumentException("invalid literal at " + pos);
      }
      pos += w.length();
    }

    String readString() {
      expect('"');
      StringBuilder sb = new StringBuilder();
      while (true) {
        char c = next();
        if (c == '"') {
          return sb.toString();
        }
        if (c < 0x20) {
          throw new IllegalArgumentException("invalid character in string at " + (pos - 1));
        }
        if (c != '\\') {
          sb.append(c);
          continue;
        }
        char e = next();
        switch (e) {
        case '"':
        case '\\':
        case '/':
          sb.append(e);
          break;
        case 'b':
          sb.append('\b');
          break;
        case 'f':
          sb.append('\f');
          break;
        case 'n':
          sb.append('\n');
          break;
        case 'r':
          sb.append('\r');
          break;
        case 't':
          sb.append('\t');
          break;
        case 'u':
          if (pos + 4 > s.length()) {
            throw new IllegalArgumentException("unexpected end of JSON input");
          }
          sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
          pos += 4;
          break;
        default:
          throw new IllegalArgumentException("invalid escape at " + (pos - 1));
        }
      }
    }
  }
}
