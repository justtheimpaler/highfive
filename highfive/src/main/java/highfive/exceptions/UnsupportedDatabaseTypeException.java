package highfive.exceptions;

public class UnsupportedDatabaseTypeException extends ApplicationException {

  private static final long serialVersionUID = 1L;

  public UnsupportedDatabaseTypeException(String message) {
    super(message);
  }

}
