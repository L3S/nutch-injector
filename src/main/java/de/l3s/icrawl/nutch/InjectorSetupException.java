package de.l3s.icrawl.nutch;

/** The injector module is not setup correctly. */
public class InjectorSetupException extends InjectorException {

    private static final long serialVersionUID = 1L;

    public InjectorSetupException(String message) {
        super(message);
    }

    public InjectorSetupException(Throwable cause) {
        super(cause);
    }

    public InjectorSetupException(String message, Throwable cause) {
        super(message, cause);
    }

}
