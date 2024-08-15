package highfive.exceptions;

public class CouldNotHashException extends ApplicationException {

  private static final long serialVersionUID = 1L;

  public CouldNotHashException(String message) {
    super(message);
  }

}