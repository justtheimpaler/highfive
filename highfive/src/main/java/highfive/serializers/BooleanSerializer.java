package highfive.serializers;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class BooleanSerializer extends Serializer<Boolean> {

  private static final byte[] FALSE = { (byte) 0xa7 };
  private static final byte[] TRUE = { (byte) 0x52 };

  private Boolean value;

  public BooleanSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getBoolean(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    return this.value ? TRUE : FALSE;
  }

  @Override
  public Boolean getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setBoolean(ordinal, value);
  }

}
