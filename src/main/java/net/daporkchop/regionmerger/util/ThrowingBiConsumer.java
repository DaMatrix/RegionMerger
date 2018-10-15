package net.daporkchop.regionmerger.util;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface ThrowingBiConsumer<A, B, E extends Exception> extends BiConsumer<A, B> {
    @Override
    default void accept(A a, B b) {
        try {
            this.realAccept(a, b);
        } catch (Exception e)   {
            throw new RuntimeException(e);
        }
    }

    void realAccept(A a, B b) throws E;
}
