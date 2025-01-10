package highfive.model;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import highfive.serializers.BigDecimalSerializer;
import highfive.serializers.BigIntegerSerializer;
import highfive.serializers.BooleanSerializer;
import highfive.serializers.ByteArraySerializer;
import highfive.serializers.DoubleSerializer;
import highfive.serializers.IntegerSerializer;
import highfive.serializers.LocalDateSerializer;
import highfive.serializers.LocalDateTimeSerializer;
import highfive.serializers.LocalTimeSerializer;
import highfive.serializers.LongSerializer;
import highfive.serializers.OffsetDateTimeSerializer;
import highfive.serializers.StringSerializer;
import highfive.serializers.ZonedDateTimeSerializer;

public abstract class Serializer<T> {

  private String name;
  private boolean canUseACollation;

  protected Serializer(boolean canUseACollation) {
    String sn = this.getClass().getSimpleName();
    this.name = (sn.substring(0, sn.length() - "Serializer".length())).toLowerCase();
    this.canUseACollation = canUseACollation;
  }

  public static Map<String, Serializer<?>> ALL = new HashMap<>();

  private static void add(final Serializer<?>... serializers) {
    for (Serializer<?> s : serializers) {
      ALL.put(s.name, s);
    }
  }

  static {
    add(new BigDecimalSerializer(), //
        new BigIntegerSerializer(), //
        new BooleanSerializer(), //
        new ByteArraySerializer(), //
        new DoubleSerializer(), //
        new IntegerSerializer(), //
        new LocalDateSerializer(), //
        new LocalDateTimeSerializer(), //
        new LocalTimeSerializer(), //
        new LongSerializer(), //
        new OffsetDateTimeSerializer(), //
        new StringSerializer(), //
        new ZonedDateTimeSerializer());
  }

  public static Serializer<?> find(final String name) {
    if (name == null) {
      return null;
    }
    return ALL.get(name.toLowerCase());
  }

  public String getName() {
    return name;
  }

  public boolean canUseACollation() {
    return canUseACollation;
  }

  public abstract T getValue();

  public abstract byte[] read(ResultSet rs, int ordinal) throws SQLException;

  public abstract void set(PreparedStatement ps, int ordinal) throws SQLException;

}
