package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import highfive.model.Serializer;

public class LongSerializer extends Serializer<Long> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private Long value;

  public LongSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getLong(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    LB.putLong(0, this.value);
    return LB.array();
  }

  @Override
  public Long getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    if (this.value == null) {
      ps.setNull(ordinal, Types.BIGINT);
    } else {
      ps.setLong(ordinal, value);
    }
  }

}
