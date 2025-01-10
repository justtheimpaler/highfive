package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class DoubleSerializer extends Serializer<Double> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private Double value;

  public DoubleSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getDouble(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    LB.putLong(0, Double.doubleToLongBits(this.value));
    return LB.array();
  }

  @Override
  public Double getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setDouble(ordinal, value);
  }

}
