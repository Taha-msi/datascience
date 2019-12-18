package ktp.isl.producers.utils;
@FunctionalInterface
public interface Converter<F,T> {
    T convert(F from);
}
