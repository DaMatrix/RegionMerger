package net.daporkchop.regionmerger.util;

/**
 * @author DaPorkchop_
 */
public interface VoidFunction<T extends Throwable> {
    void run() throws T;
}
