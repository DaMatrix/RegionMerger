/*
 * Adapted from The MIT License (MIT)
 *
 * Copyright (c) 2018-2020 DaPorkchop_
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
 */

package net.daporkchop.regionmerger.option;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import net.daporkchop.regionmerger.World;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author DaPorkchop_
 */
public interface Option<V> {
    Option<World> DESTINATION = new Option<World>() {
        @Override
        public String name() {
            return "dst";
        }

        @Override
        public Type type() {
            return Type.FLAG;
        }

        @Override
        public World parse(@NonNull String word, @NonNull Iterator<String> itr) {
            return new World(new File(word), false);
        }

        @Override
        public World fallbackValue() {
            throw new IllegalArgumentException("No destination path given!");
        }
    };

    Option<List<World>> SOURCES = new Option<List<World>>() {
        @Override
        public String name() {
            return "dst";
        }

        @Override
        public Type type() {
            return Type.FLAG;
        }

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

    static Flag flag(@NonNull String name)  {
        return new Flag(name);
    }

    static Int integer(@NonNull String name, int fallback, int min, int max)  {
        return new Int(name, fallback, min, max);
    }

    /**
     * @return this option's name
     */
    String name();

    /**
     * @return this option's {@link Type}
     */
    Type type();

    /**
     * Parses a value when this option is found.
     *
     * @param word the text that triggered this option
     * @param itr an {@link Iterator} over the program arguments. This may be used to obtain additional parameters
     * @return a value
     */
    V parse(@NonNull String word, @NonNull Iterator<String> itr);

    /**
     * Returns the fallback value of this parameter if it is not given.
     *
     * @return the fallback value of this parameter if it is not given
     */
    V fallbackValue();

    enum Type {
        FLAG,
        INT,
        DESTINATION,
        SOURCES;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    @Accessors(fluent = true)
    final class Flag implements Option<Boolean> {
        @NonNull
        protected final String name;

        @Override
        public Type type() {
            return Type.FLAG;
        }

        @Override
        public Boolean parse(@NonNull String word, @NonNull Iterator<String> itr) {
            return Boolean.TRUE;
        }

        @Override
        public Boolean fallbackValue() {
            return Boolean.FALSE;
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    @Accessors(fluent = true)
    final class Int implements Option<Integer> {
        @NonNull
        protected final String name;
        protected final Integer fallback;
        protected final int min;
        protected final int max;

        @Override
        public Type type() {
            return Type.INT;
        }

        @Override
        public Integer parse(@NonNull String word, @NonNull Iterator<String> itr) {
            int val = Integer.parseInt(itr.next());
            if (val < this.min || val > this.max)   {
                throw new IllegalArgumentException(String.format("Invalid value '%d'! Must be in range %d-%d (inclusive).", val, this.min, this.max));
            }
            return val;
        }

        @Override
        public Integer fallbackValue() {
            return this.fallback;
        }
    }
}
