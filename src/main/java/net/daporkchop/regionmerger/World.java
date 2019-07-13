/*
 * Adapted from the Wizardry License
 *
 * Copyright (c) 2018-2019 DaPorkchop_ and contributors
 *
 * Permission is hereby granted to any persons and/or organizations using this software to copy, modify, merge, publish, and distribute it. Said persons and/or organizations are not allowed to use the software or any derivatives of the work for commercial use or any other means to generate income, nor are they allowed to claim this software as their own.
 *
 * The persons and/or organizations are also disallowed from sub-licensing and/or trademarking this software without explicit permission from DaPorkchop_.
 *
 * Any persons and/or organizations using this software must disclose their source code and have it publicly available, include this license, provide sufficient credit to the original authors of the project (IE: DaPorkchop_), as well as provide a link to the original project.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NON INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

package net.daporkchop.regionmerger;

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
    protected final File              regionDir;
    protected final boolean           readOnly;

    public World(@NonNull File path, boolean readOnly) {
        this.path = path;
        this.regionDir = new File(path, "region/");
        this.readOnly = readOnly;

        if (!path.exists()) {
            if (readOnly) {
                throw new IllegalStateException(String.format("World \"%s\" doesn't exist!", path.getAbsolutePath()));
            }
            PFiles.ensureDirectoryExists(this.regionDir);
        }
        this.regions = Arrays.stream(this.regionDir.listFiles())
                             .filter(File::isFile)
                             .map(File::getName)
                             .map(REGION_PATTERN::matcher)
                             .filter(Matcher::matches)
                             .map(m -> new Vec2i(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2))))
                             .collect(Collectors.toSet());
    }

    public File getAsFile(@NonNull Vec2i regionPos) {
        return new File(this.regionDir, String.format("r.%d.%d.mca", regionPos.getX(), regionPos.getY()));
    }
}
