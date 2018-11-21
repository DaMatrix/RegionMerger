package net.daporkchop.regionmerger.random;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import net.daporkchop.regionmerger.util.Pos;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.Math.sqrt;

public class ChunkDistributionGenerator {
    private static final int step = 2000;
    private static final int step_d = step;

    public static void main(String... args) throws IOException {
        Collection<Pos> positions;
        try (Reader reader = new InputStreamReader(new FileInputStream(new File("/media/daporkchop/400Gb/missingchunks_whole.json")))) {
            positions = StreamSupport.stream(new JsonParser().parse(reader).getAsJsonArray().spliterator(), true)
                    .map(JsonElement::getAsJsonObject)
                    .map(obj -> new Pos(obj.get("x").getAsInt() << 4, obj.get("z").getAsInt() << 4))
                    .collect(Collectors.toSet());
        }
        Map<Integer, AtomicInteger> valuesMap = new Hashtable<>();
        positions.stream()
                .map(pos -> sqrt(pow(pos.x, 2.0d) + pow(pos.z, 2.0d)))
                //.sorted(Comparator.comparingDouble(d -> d))
                .map(d -> d / step_d)
                .map(Math::round)
                .map(d -> (int) (long) d)
                .forEach(i -> valuesMap.computeIfAbsent(i, j -> new AtomicInteger()).incrementAndGet());
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        valuesMap.forEach((i, count) -> dataset.addValue(count.get(), "missing", String.format("%dk", i * step)));

        JFreeChart lineChartObject = ChartFactory.createLineChart(
                "Missing chunks","Distance from spawn",
                "Missing chunks Count",
                dataset, PlotOrientation.VERTICAL,
                true,true,false);

        File lineChart = new File( "missing.png" );
        ChartUtils.saveChartAsJPEG(lineChart ,lineChartObject, 1920, 1080);
    }
}
