package highfive.utils;

import java.sql.Connection;
import java.sql.SQLException;

public class Utl {

  private static final char[] DIGITS = "0123456789abcdef".toCharArray();

  public static boolean distinct(final String a, final String b) {
    return a == null ? b != null : !a.equals(b);
  }

  public static boolean empty(final String s) {
    return s == null || s.trim().isEmpty();
  }

  public static String toHex(final byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    char[] result = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      result[j * 2] = DIGITS[v >>> 4];
      result[j * 2 + 1] = DIGITS[v & 0x0F];
    }
    return new String(result);
  }

  public static String coalesce(final String... strings) {
    for (String s : strings) {
      if (s != null) {
        return s;
      }
    }
    return null;
  }

  public static String isClosed(final Connection conn) {
    try {
      return conn.isClosed() ? "closed" : "open";
    } catch (SQLException e) {
      e.printStackTrace();
      return "?";
    }
  }

}
