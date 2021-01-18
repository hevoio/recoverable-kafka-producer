package com.hevodata.commons;

import lombok.NoArgsConstructor;
import lombok.Setter;

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