package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import highfive.model.Serializer;

public class LocalDateTimeSerializer extends Serializer<LocalDateTime> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private LocalDateTime value;

  public LocalDateTimeSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getObject(ordinal, LocalDateTime.class);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    LB.putLong(0, this.value.toEpochSecond(ZoneOffset.UTC));
    return LB.array();
  }

  @Override
  public LocalDateTime getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setObject(ordinal, value);
  }

}
