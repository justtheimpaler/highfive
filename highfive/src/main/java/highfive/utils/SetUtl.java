package highfive.utils;

import java.util.Set;
import java.util.TreeSet;

public class SetUtl {

  public static <T> Set<T> union(Set<T> a, Set<T> b) {
    Set<T> c = new TreeSet<T>(a);
    c.addAll(b);
    return c;
  }

  public static <T> Set<T> intersection(Set<T> a, Set<T> b) {
    Set<T> c = new TreeSet<T>(a);
    c.retainAll(b);
    return c;
  }

  public static <T> Set<T> difference(Set<T> a, Set<T> b) {
    Set<T> c = new TreeSet<T>(a);
    c.removeAll(b);
    return c;
  }

}
