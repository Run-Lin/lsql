package com.r.spark.repl.sql.exception;



public class SessionNotFoundException extends LivyException {

    public SessionNotFoundException(String message) {
        super(message);
    }
}