package net.daporkchop.regionmerger;

import net.daporkchop.regionmerger.util.Pos;
import net.daporkchop.regionmerger.util.RegionFile;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;

public class World {
    public final File root;
    public final File regions;
    public final Collection<Pos> ownedRegions = new ArrayDeque<>();

    public World(File file) {
        Objects.requireNonNull(file);

        this.root = file;
        this.regions = new File(file, "region");
        if (!this.regions.exists())  {
            throw new IllegalStateException("No region folder!");
        }
        File[] files = this.regions.listFiles();
        if (files == null)  {
            throw new NullPointerException();
        }
        for (File f : files)    {
            String name = f.getName();
            if (name.endsWith(".mca") && name.startsWith("r.")) {
                String[] split = name.split("\\.");
                int x = Integer.parseInt(split[1]);
                int z = Integer.parseInt(split[2]);
                this.ownedRegions.add(new Pos(x, z));
            }
        }
    }

    public RegionFile getRegion(Pos pos)    {
        return this.getRegion(pos.x, pos.z);
    }

    public RegionFile getRegion(int x, int y)    {
        return new RegionFile(new File(this.regions, String.format("r.%d.%d.mca", x, y)));
    }
}
