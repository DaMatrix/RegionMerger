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

package net.daporkchop.regionmerger.util;

import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import net.daporkchop.lib.common.misc.file.PFiles;
import net.daporkchop.lib.math.vector.i.Vec2i;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author DaPorkchop_
 */
@Getter
@Accessors(fluent = true)
public class World {
    protected static final Pattern REGION_PATTERN = Pattern.compile("r\\.([-0-9]+)\\.([-0-9]+)\\.mca");

    protected final Collection<Vec2i> regions;
    protected final File              path;
    protected final boolean           readOnly;

    public World(@NonNull File path, boolean readOnly) {
        this.path = path;
        this.readOnly = readOnly;

        if (!path.exists() && readOnly) {
            throw new IllegalStateException(String.format("World \"%s\" doesn't exist!", path.getAbsolutePath()));
        }
        PFiles.ensureDirectoryExists(this.path);
        this.regions = Arrays.stream(this.path.listFiles())
                .filter(File::isFile)
                .map(File::getName)
                .map(REGION_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(m -> new Vec2i(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))))
                .collect(Collectors.toSet());
    }

    public File getAsFile(@NonNull Vec2i regionPos) {
        return new File(this.path, String.format("r.%d.%d.mca", regionPos.getX(), regionPos.getY()));
    }
}
