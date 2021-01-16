/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2018-2021 DaPorkchop_
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following conditions:
 *
 * Any persons and/or organizations using this software must include the above copyright notice and this permission notice,
 * provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.regionmerger.option;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.daporkchop.lib.common.misc.string.PStrings;
import net.daporkchop.regionmerger.util.World;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
public interface Option<V> {
    Option<World> DESTINATION = new BaseOption<World>("dst") {
        @Override
        public World parse(@NonNull String word, @NonNull Iterator<String> itr) {
            return new World(new File(word), false);
        }

        @Override
        public World fallbackValue() {
            throw new IllegalArgumentException("No destination path given!");
        }
    };

    Option<List<World>> SOURCES = new BaseOption<List<World>>("src") {
        @Override
        public List<World> parse(@NonNull String word, @NonNull Iterator<String> itr) {
            List<World> sources = new LinkedList<>();
            do {
                sources.add(new World(new File(word), true));
            } while (itr.hasNext() && (word = itr.next()) != null);
            return new ArrayList<>(sources);
        }

        @Override
        public List<World> fallbackValue() {
            return Collections.emptyList();
        }
    };

    static Option<Boolean> flag(@NonNull String name) {
        return new BaseOption<Boolean>(name) {
            @Override
            public Boolean parse(@NonNull String word, @NonNull Iterator<String> itr) {
                return Boolean.TRUE;
            }

            @Override
            public Boolean fallbackValue() {
                return Boolean.FALSE;
            }
        };
    }

    static Option<Integer> integer(@NonNull String name, Integer fallback) {
        return integer(name, fallback, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    static Option<Integer> integer(@NonNull String name, Integer fallback, int min, int max) {
        return new BaseOption<Integer>(name) {
            @Override
            public Integer parse(@NonNull String word, @NonNull Iterator<String> itr) {
                int val = Integer.parseInt(itr.next());
                if (val < min || val > max) {
                    throw new IllegalArgumentException(String.format("Invalid value '%d'! Must be in range %d-%d (inclusive).", val, min, max));
                }
                return val;
            }

            @Override
            public Integer fallbackValue() {
                return fallback;
            }
        };
    }

    static Option<String> text(@NonNull String name, String fallback) {
        return new BaseOption<String>(name) {
            @Override
            public String parse(@NonNull String word, @NonNull Iterator<String> itr) {
                return itr.next();
            }

            @Override
            public String fallbackValue() {
                return fallback;
            }
        };
    }

    static <E extends Enum<E>> Option<E> ofEnum(@NonNull String name, @NonNull Class<E> type, E fallback) {
        return new BaseOption<E>(name) {
            @Override
            public E parse(@NonNull String word, @NonNull Iterator<String> itr) {
                String text = itr.next();
                return Arrays.stream(type.getEnumConstants())
                        .filter(value -> value.name().equalsIgnoreCase(text))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException(PStrings.fastFormat(
                                "Invalid enum type name: \"%s\" (expected one of %s)",
                                text,
                                Arrays.stream(type.getEnumConstants()).map(Enum::name).collect(Collectors.joining(", ", "[", "]")))));
            }

            @Override
            public E fallbackValue() {
                return fallback;
            }
        };
    }

    /**
     * @return this option's name
     */
    String name();

    /**
     * Parses a value when this option is found.
     *
     * @param word the text that triggered this option
     * @param itr  an {@link Iterator} over the program arguments. This may be used to obtain additional parameters
     * @return a value
     */
    V parse(@NonNull String word, @NonNull Iterator<String> itr);

    /**
     * Returns the fallback value of this parameter if it is not given.
     *
     * @return the fallback value of this parameter if it is not given
     */
    V fallbackValue();

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    @Getter
    @Accessors(fluent = true)
    abstract class BaseOption<T> implements Option<T> {
        @NonNull
        protected final String name;

        @Override
        public int hashCode() {
            return this.name().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj instanceof String) {
                return this.name().equals(obj);
            } else if (obj instanceof Option) {
                return this.name().equals(((Option<?>) obj).name());
            } else {
                return false;
            }
        }
    }
}
