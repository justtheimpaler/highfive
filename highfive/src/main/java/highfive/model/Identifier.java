package highfive.model;

public class Identifier {

  protected String canonicalName;
  protected String genericName;
  private Dialect dialect;

  public Identifier(String canonicalName, String removeTablePrefix, Dialect dialect) {
    this.canonicalName = canonicalName;
    String name = this.canonicalName.toLowerCase();
    if (removeTablePrefix != null && name.startsWith(removeTablePrefix)) {
      this.genericName = name.substring(removeTablePrefix.length());
    } else {
      this.genericName = name;
    }
    this.dialect = dialect;
  }

  public String getCanonicalName() {
    return this.canonicalName;
  }

  public String getGenericName() {
    return this.genericName;
  }

  public String renderSQL() {
    return this.dialect.escapeIdentifierAsNeeded(this.canonicalName);
  }

}
