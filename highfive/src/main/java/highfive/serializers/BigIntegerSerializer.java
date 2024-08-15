package highfive.serializers;

import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class BigIntegerSerializer extends Serializer<BigInteger> {

  private BigInteger value;

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getObject(ordinal, BigInteger.class);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    return this.value.toByteArray();
  }

  @Override
  public BigInteger getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setObject(ordinal, value);
  }

}
