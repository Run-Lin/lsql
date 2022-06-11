package com.r.spark.repl.sql.exception;

/**
 * APINotFoundException happens because we may introduce new apis in new livy version.
 */
public class APINotFoundException extends LivyException {
    public APINotFoundException() {
    }

    public APINotFoundException(String message) {
        super(message);
    }

    public APINotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public APINotFoundException(Throwable cause) {
        super(cause);
    }

    public APINotFoundException(String message, Throwable cause, boolean enableSuppression,
                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}