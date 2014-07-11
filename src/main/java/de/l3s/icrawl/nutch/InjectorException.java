package de.l3s.icrawl.nutch;

/** Base class for all exceptions in the injector module. */
public class InjectorException extends Exception {
    private static final long serialVersionUID = 1L;

    public InjectorException(String message) {
        super(message);
    }

    public InjectorException(Throwable cause) {
        super(cause);
    }

    public InjectorException(String message, Throwable cause) {
        super(message, cause);
    }


}
