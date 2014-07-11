package de.l3s.icrawl.nutch;

/** An error happend during the injection of an URL. */
public class InjectorInjectionException extends InjectorException {

    private static final long serialVersionUID = 1L;

    public InjectorInjectionException(String message) {
        super(message);
    }

    public InjectorInjectionException(Throwable cause) {
        super(cause);
    }

    public InjectorInjectionException(String message, Throwable cause) {
        super(message, cause);
    }

}
