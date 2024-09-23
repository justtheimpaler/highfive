package highfive.utils;

public class Name {

  private boolean quoted;
  private String name;

  public static Name of(String s) {
    if (s == null) {
      return null;
    }
    Name name = new Name();
    if (s.startsWith("\"") && s.endsWith("\"")) {
      name.quoted = true;
      name.name = s.substring(1, s.length() - 1);
    } else {
      name.quoted = false;
      name.name = s.toLowerCase();
    }
    return name;
  }

  public static String lower(String name) {
    if (name == null) {
      return null;
    }
    if (name.startsWith("\"") && name.endsWith("\"")) {
      return name;
    }
    return name.toLowerCase();
  }

  public boolean isQuoted() {
    return quoted;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return this.isQuoted() ? ("\"" + this.name + "\"") : this.name;
  }

  // Indexable

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + (quoted ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Name other = (Name) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (quoted != other.quoted)
      return false;
    return true;
  }

}
