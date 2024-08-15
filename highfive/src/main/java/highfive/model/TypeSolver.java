package highfive.model;

import java.util.HashMap;
import java.util.Map;

import highfive.exceptions.JavaTypeNotSupportedException;
import highfive.utils.Utl;

public class TypeSolver {

  private Map<String, Serializer<?>> rules = new HashMap<>();

  public void add(final String type, final String serializerName) throws JavaTypeNotSupportedException {
    if (Utl.empty(type)) {
      throw new RuntimeException("Cannot add a rule with no type.");
    }
    if (Utl.empty(serializerName)) {
      throw new RuntimeException("Cannot add a rule with no serializer name.");
    }
    String javaType = serializerName.trim().toLowerCase();
    Serializer<?> s = Serializer.find(javaType);
    if (s == null) {
      throw new JavaTypeNotSupportedException("Java type '" + javaType + "' is not supported in the type rules.");
    }
    this.rules.put(type.trim().toLowerCase(), s);
  }

  public Serializer<?> resolve(final String renderedType) {
    if (renderedType == null) {
      return null;
    }
    return this.rules.get(renderedType.toLowerCase());
  }

}
