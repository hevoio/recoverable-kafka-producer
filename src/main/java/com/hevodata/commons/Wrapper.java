package com.hevodata.commons;

import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Created by Wiki on 28/01/20.
 */
@Setter
@NoArgsConstructor
public class Wrapper<T> {

    T data;

    public Wrapper(T data) {
        this.data = data;
    }

    public T get() {
        return this.data;
    }

    public static <T> Wrapper<T> of(T value) {
        return new Wrapper<>(value);
    }

}