package com.r.spark.repl.sql.exception;


/**
 * Livy api related exception.
 */
public class LivyException extends InterpreterException {
    public LivyException() {
    }

    public LivyException(String message) {
        super(message);
    }

    public LivyException(String message, Throwable cause) {
        super(message, cause);
    }

    public LivyException(Throwable cause) {
        super(cause);
    }

    public LivyException(String message, Throwable cause, boolean enableSuppression,
                         boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
