package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;

import highfive.model.Serializer;

public class OffsetDateTimeSerializer extends Serializer<OffsetDateTime> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private OffsetDateTime value;

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    rs.getObject(ordinal); // deals with the DB2 JDBC driver that doesn't handle nulls properly
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    this.value = rs.getObject(ordinal, OffsetDateTime.class);
    LB.putLong(0, this.value.toEpochSecond());
    return LB.array();
  }

  @Override
  public OffsetDateTime getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setObject(ordinal, value);
  }

}
