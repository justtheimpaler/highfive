package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;

import highfive.model.Serializer;

public class LocalTimeSerializer extends Serializer<LocalTime> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private LocalTime value;

  public LocalTimeSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    rs.getObject(ordinal); // deals with the DB2 JDBC driver that doesn't handle nulls properly
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    this.value = rs.getObject(ordinal, LocalTime.class);
    LB.putLong(0, this.value.toNanoOfDay());
    return LB.array();
  }

  @Override
  public LocalTime getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setObject(ordinal, value);
  }

}
