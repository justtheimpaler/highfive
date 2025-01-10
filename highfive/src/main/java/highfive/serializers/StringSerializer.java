package highfive.serializers;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import highfive.model.Serializer;

public class StringSerializer extends Serializer<String> {

  private String value;

  public StringSerializer() {
    super(true);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getString(ordinal);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    return this.value.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setString(ordinal, value);
  }

}
