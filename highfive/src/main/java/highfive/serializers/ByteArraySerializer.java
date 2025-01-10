package highfive.serializers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class ByteArraySerializer extends Serializer<byte[]> {

  private byte[] value;

  public ByteArraySerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getBytes(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    return this.value;
  }

  @Override
  public byte[] getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setBytes(ordinal, value);
  }

}
