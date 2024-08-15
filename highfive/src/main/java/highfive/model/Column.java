package highfive.model;

import java.math.BigInteger;

public class Column implements Comparable<Column> {

  private String name;
  private String canonicalName;
  private String type;
  private BigInteger length;
  private Integer precision;
  private Integer scale;
  private String renderedType;
  private Integer pkPosition;
  private Serializer<?> serializer;

  public Column(String name, String type, BigInteger length, Integer precision, Integer scale, String renderedType,
      Integer pkPosition, Serializer<?> serializer) {
    this.name = name.toLowerCase();
    this.canonicalName = name;
    this.type = type.toLowerCase();
    this.length = length;
    this.precision = precision;
    this.scale = scale;
    this.renderedType = renderedType;
    this.pkPosition = pkPosition;
    this.serializer = serializer;
  }

  public String getName() {
    return name;
  }

  public String getCanonicalName() {
    return canonicalName;
  }

  public String getType() {
    return type;
  }

  public BigInteger getLength() {
    return length;
  }

  public Integer getPrecision() {
    return precision;
  }

  public Integer getScale() {
    return scale;
  }

  public String getRenderedType() {
    return renderedType;
  }

  public Integer getPKPosition() {
    return pkPosition;
  }

  public Serializer<?> getSerializer() {
    return serializer;
  }

  @Override
  public int compareTo(Column o) {
    return this.name.compareTo(o.name);
  }

}
