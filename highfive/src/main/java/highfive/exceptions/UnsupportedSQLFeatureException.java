package highfive.exceptions;

public class UnsupportedSQLFeatureException extends ApplicationException {

  private static final long serialVersionUID = 1L;

  public UnsupportedSQLFeatureException(String message) {
    super(message);
  }

}
