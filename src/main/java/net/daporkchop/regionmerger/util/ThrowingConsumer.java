package net.daporkchop.regionmerger.util;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@FunctionalInterface
public interface ThrowingConsumer<A, E extends Exception> extends Consumer<A> {
    @Override
    default void accept(A a)    {
        try {
            this.realAccept(a);
        } catch (Exception e)   {
            throw new RuntimeException(e);
        }
    }

    void realAccept(A a) throws E;
}
