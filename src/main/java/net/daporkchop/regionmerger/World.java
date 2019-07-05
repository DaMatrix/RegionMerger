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
 */

package net.daporkchop.regionmerger;

import lombok.NonNull;
import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.anvil.mojang.RegionFile;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;

public class World {
    public final File root;
    public final File regions;
    public final Collection<Pos> ownedRegions = new ArrayDeque<>();
    public final String extension;

    public World(File file) {
        this(file, "mca", false);
    }

    public World(@NonNull File file, @NonNull String extension, boolean create) {
        this.extension = extension;
        this.root = file;
        this.regions = new File(file, "region");
        if (!this.regions.exists()) {
            if (create) {
                if (!this.regions.mkdirs()) {
                    throw new IllegalStateException(String.format("Could not create folder: %s", this.regions.getAbsolutePath()));
                }
            } else {
                throw new IllegalStateException("No region folder in " + this.regions.getAbsolutePath());
            }
        }
        File[] files = this.regions.listFiles();
        if (files == null) {
            throw new NullPointerException();
        }
        for (File f : files) {
            String name = f.getName();
            if (name.endsWith(String.format(".%s", extension)) && name.startsWith("r.")) {
                String[] split = name.split("\\.");
                int x = Integer.parseInt(split[1]);
                int z = Integer.parseInt(split[2]);
                this.ownedRegions.add(new Pos(x, z));
            }
        }
    }

    public RegionFile getRegion(Pos pos) {
        return this.getRegion(pos, true);
    }

    public RegionFile getRegion(Pos pos, boolean checkValid) {
        if (checkValid && !this.ownedRegions.contains(pos)) {
            throw new IllegalArgumentException(String.format("World %s does not own region (%d,%d)", this.root.getName(), pos.x, pos.z));
        }
        return new RegionFile(new File(this.regions, String.format("r.%d.%d.%s", pos.x, pos.z, this.extension)));
    }

    public RegionFile getRegion(int x, int z) {
        return this.getRegion(new Pos(x, z));
    }

    public RegionFile getRegionOrNull(Pos pos) {
        if (!this.ownedRegions.contains(pos)) {
            return null;
        }
        return new RegionFile(new File(this.regions, String.format("r.%d.%d.%s", pos.x, pos.z, this.extension)));
    }

    public RegionFile getOrCreateRegion(Pos pos) {
        if (!this.ownedRegions.contains(pos)) {
            this.ownedRegions.add(pos);
        }
        return new RegionFile(new File(this.regions, String.format("r.%d.%d.%s", pos.x, pos.z, this.extension)));
    }

    public File getActualFileForRegion(Pos pos) {
        return new File(this.regions, String.format("r.%d.%d.%s", pos.x, pos.z, this.extension));
    }
}
