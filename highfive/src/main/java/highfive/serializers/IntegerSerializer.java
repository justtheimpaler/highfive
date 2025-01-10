package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import highfive.model.Serializer;

public class IntegerSerializer extends Serializer<Integer> {

  private static ByteBuffer IB = ByteBuffer.allocate(Integer.BYTES);

  private Integer value;

  public IntegerSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getInt(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    IB.putInt(0, this.value);
    return IB.array();
  }

  @Override
  public Integer getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    if (value == null) {
      ps.setNull(ordinal, Types.INTEGER);
    } else {
      ps.setInt(ordinal, value);
    }
  }

}
