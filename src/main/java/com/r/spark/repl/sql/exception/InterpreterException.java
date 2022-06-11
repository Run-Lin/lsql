package com.r.spark.repl.sql.exception;


public class InterpreterException extends Exception {

    public InterpreterException() {
    }

    public InterpreterException(Throwable e) {
        super(e);
    }

    public InterpreterException(String m) {
        super(m);
    }

    public InterpreterException(String msg, Throwable t) {
        super(msg, t);
    }

    public InterpreterException(String message, Throwable cause, boolean enableSuppression,
                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
