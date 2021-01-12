package com.hevodata.commons;

@FunctionalInterface
public interface ThrowingConsumer<T> {

    void accept(T t) throws Exception;


}
