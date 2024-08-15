package highfive.serializers;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class BigDecimalSerializer extends Serializer<BigDecimal> {

  private BigDecimal value;

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getBigDecimal(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    return this.value.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public BigDecimal getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setBigDecimal(ordinal, value);
  }

}
