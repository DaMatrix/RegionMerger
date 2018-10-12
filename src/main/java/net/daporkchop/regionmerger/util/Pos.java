package net.daporkchop.regionmerger.util;

public class Pos {
    public final int x;
    public final int z;

    public Pos(int x, int z)     {
        this.x = x;
        this.z = z;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pos) {
            Pos pos = (Pos) obj;
            return this.x == pos.x && this.z == pos.z;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.x * 89347523 ^ this.z * 39723745;
    }

    @Override
    public String toString() {
        return String.format("Pos(x=%d,z=%d)", this.x, this.z);
    }
}
