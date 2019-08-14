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

package net.daporkchop.regionmerger.mode;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.NonNull;
import net.daporkchop.lib.common.function.io.IOConsumer;
import net.daporkchop.lib.logging.Logger;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.World;
import net.daporkchop.regionmerger.option.Arguments;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author DaPorkchop_
 */
public class Patch implements Mode {
    @Override
    public void printUsage(@NonNull Logger logger) {
    }

    @Override
    public Arguments arguments() {
        return new Arguments(true, true);
    }

    @Override
    public String name() {
        return "patch";
    }

    @Override
    public void run(@NonNull Arguments args) throws IOException {
        final World dst = args.getDestination();
        final List<World> sources = args.getSources();

        Collection<Vec2i> regionPositions = sources.stream()
                .map(World::regions)
                .flatMap(Collection::stream)
                .distinct()
                .collect(Collectors.toSet());

        logger.info("Loaded output world with %d existing regions.", dst.regions().size());
        logger.info("Loaded %d input worlds with a total of %d distinct regions.", sources.size(), regionPositions.size());

        Collection<Vec2i> missingChunks;
        try (Reader reader = new InputStreamReader(new BufferedInputStream(new FileInputStream(new File("missingchunks.json")))))   {
            missingChunks = StreamSupport.stream(new JsonParser().parse(reader).getAsJsonArray().spliterator(), false)
                    .map(JsonElement::getAsJsonObject)
                    .map(obj -> new Vec2i(obj.get("x").getAsInt(), obj.get("z").getAsInt()))
                    .collect(Collectors.toSet());
        }
        Collection<Vec2i> missingRegions = missingChunks.stream()
                .map(pos -> new Vec2i(pos.getX() >> 5, pos.getY() >> 5))
                .distinct()
                .collect(Collectors.toSet());

        logger.info("Loaded %d missing chunk positions in %d regions.", missingChunks.size(), missingRegions.size())
        .info("Backing up regions...");

        missingRegions.parallelStream()
                .map(dst::getAsFile)
                .filter(File::exists)
                .forEach((IOConsumer<File>) file -> {
                    int len = (int) file.length();
                    ByteBuf buf = PooledByteBufAllocator.DEFAULT.ioBuffer(len);
                    try {
                        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                            if (buf.writeBytes(channel, len) != len) {
                                throw new IllegalStateException(String.format("Couldn't read %d bytes!", len));
                            }
                        }
                        try (FileChannel channel = FileChannel.open(new File(file.getPath() + ".bak").toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                            if (buf.readBytes(channel, len) != len) {
                                throw new IllegalStateException(String.format("Couldn't write %d bytes!", len));
                            }
                        }
                    } finally {
                        buf.release();
                    }
                });

        logger.info("Searching for regions containing the missing chunks...");
    }
}
