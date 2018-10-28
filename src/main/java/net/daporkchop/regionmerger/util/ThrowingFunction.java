package net.daporkchop.regionmerger.util;

import java.util.function.Function;

/**
 * @author DaPorkchop_
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Throwable> extends Function<T, R> {
    @Override
    default R apply(T t) {
        try {
            return this.doApply(t);
        } catch (Throwable e)   {
            throw new RuntimeException(e);
        }
    }

    R doApply(T t) throws E;
}
