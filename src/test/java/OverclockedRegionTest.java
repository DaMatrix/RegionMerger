import lombok.NonNull;
import net.daporkchop.lib.nbt.NBTIO;
import net.daporkchop.lib.nbt.tag.impl.notch.ByteArrayTag;
import net.daporkchop.lib.nbt.tag.impl.notch.CompoundTag;
import net.daporkchop.lib.nbt.tag.impl.notch.IntArrayTag;
import net.daporkchop.lib.nbt.tag.impl.notch.ListTag;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.anvil.mojang.OverclockedRegionFile;
import net.daporkchop.regionmerger.util.Pos;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author DaPorkchop_
 */
public class OverclockedRegionTest {
    //private static final File inputDir = new File("Z:\\Minecraft\\2b2t\\testworld");
    private static final File inputDir = new File("Z:\\Minecraft\\2b2t\\WorldCompressionComparison\\Spawn_PorkAnvil_v1_Uncompressed_(Default)");

    @Test
    public void test() throws IOException {
        World world = new World(inputDir);
        OverclockedRegionFile regionFile = new OverclockedRegionFile(world.getActualFileForRegion(new Pos(9, -10)));
        for (int x = 31; x >= 0; x--) {
            for (int z = 31; z >= 0; z--) {
                InputStream is = regionFile.readData(0, 0);
                if (is == null) {
                    throw new NullPointerException();
                }
                CompoundTag tag = NBTIO.read(is);
                /*try (NBTInputStream input = new NBTInputStream(is, false)) {
                    CompoundTag tag = (CompoundTag) input.readTag();
                    tag.getValue().forEach((name, t) -> System.out.printf("Tag subelement: %s\n", name));
                }*/
                this.printCompoundTag(tag, 0);
                is.close();
                if (true) {
                    regionFile.close();
                    return;
                }
            }
        }
    }

    private void printCompoundTag(@NonNull CompoundTag tag, int depth) {
        System.out.printf("%sCompound tag: %s (%d children)\n", this.space(depth - 2), tag.getName(), tag.getValue().size());
        tag.getValue().forEach((name, subtag) -> {
            if (subtag instanceof CompoundTag) {
                //System.out.printf("%sCompound tag: %s (%d children)\n", this.space(depth), name, ((CompoundTag) subtag).getValue().size());
                this.printCompoundTag((CompoundTag) subtag, depth + 2);
            } else if (subtag instanceof ListTag) {
                System.out.printf("%sList tag: %s (%d children)\n", this.space(depth), name, ((ListTag) subtag).getValue().size());
                ((ListTag<CompoundTag>) subtag).getValue().forEach(t -> this.printCompoundTag(t, depth + 2));
            } else if (subtag instanceof ByteArrayTag) {
                System.out.printf("%sbyte[]: %s (%d bytes)\n", this.space(depth), name, ((ByteArrayTag) subtag).getValue().length);
            } else if (subtag instanceof IntArrayTag) {
                System.out.printf("%sint[]: %s (%d ints)\n", this.space(depth), name, ((IntArrayTag) subtag).getValue().length);
            } else {
                System.out.printf("%sOther: %s (class=%s)\n", this.space(depth), name, subtag.getClass().getCanonicalName());
            }
        });
    }

    private String space(int depth) {
        depth += 2;
        char[] c = new char[depth];
        for (int i = depth - 1; i >= 0; i--) {
            c[i] = ' ';
        }
        return new String(c);
    }
}
