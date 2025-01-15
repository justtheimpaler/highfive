package highfive.serializers;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import highfive.model.Serializer;

public class LocalDateSerializer extends Serializer<LocalDate> {

  private static ByteBuffer LB = ByteBuffer.allocate(Long.BYTES);

  private LocalDate value;

  public LocalDateSerializer() {
    super(false);
  }

  @Override
  public byte[] read(ResultSet rs, int ordinal) throws SQLException {
    this.value = rs.getObject(ordinal, LocalDate.class);
    if (rs.wasNull()) {
      this.value = null;
      return null;
    }
    LB.putLong(0, this.value.toEpochDay());
    return LB.array();
  }

  @Override
  public LocalDate getValue() {
    return this.value;
  }

  @Override
  public void set(PreparedStatement ps, int ordinal) throws SQLException {
    ps.setObject(ordinal, value);
  }

}
