package com.relay.realtime.models;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Pojo {
    public String api_key;
    public Long l;
    public Boolean b;
    public List<String> strings;
    public Integer[] ints;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pojo pojo = (Pojo) o;
        return Objects.equals(api_key, pojo.api_key) && Objects.equals(l, pojo.l) && Objects.equals(b, pojo.b) && Objects.equals(strings, pojo.strings) && Arrays.equals(ints, pojo.ints);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(api_key, l, b, strings);
        result = 31 * result + Arrays.hashCode(ints);
        return result;
    }
}