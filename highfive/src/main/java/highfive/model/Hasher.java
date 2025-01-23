package highfive.model;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import highfive.utils.Utl;

public class Hasher {

  private static final byte[] NULL = { 123 };

  private MessageDigest digest;
  private boolean active;

  public Hasher() throws NoSuchAlgorithmException {
    this.digest = MessageDigest.getInstance("SHA-256");
    this.active = true;
  }

  public void apply(final byte[] v) {
    if (!this.active) {
      throw new RuntimeException("Hasher is already closed.");
    }
    this.digest.update(v == null ? NULL : v);
  }

  public byte[] getInProgressDigest() throws CloneNotSupportedException {
    MessageDigest c = (MessageDigest) this.digest.clone();
    return c.digest();
  }

  public String getOngoingHash() throws CloneNotSupportedException {
    return Utl.toHex(getInProgressDigest());
  }

  public byte[] close() {
    if (!this.active) {
      throw new RuntimeException("Hasher is already closed.");
    }
    this.active = false;
    return this.digest.digest();
  }

}
