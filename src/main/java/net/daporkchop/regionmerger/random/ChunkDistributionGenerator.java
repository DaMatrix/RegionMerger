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

package net.daporkchop.regionmerger.random;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import net.daporkchop.lib.math.vector.i.Vec2i;
import net.daporkchop.regionmerger.util.Pos;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.SymbolAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.LookupPaintScale;
import org.jfree.chart.renderer.xy.XYBlockRenderer;
import org.jfree.chart.title.PaintScaleLegend;
import org.jfree.chart.ui.RectangleEdge;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.DefaultXYZDataset;

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Math.*;

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
        if (false) {
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
                    "Missing chunks", "Distance from spawn",
                    "Missing chunks Count",
                    dataset, PlotOrientation.VERTICAL,
                    true, true, false);

            File lineChart = new File("missing.png");
            ChartUtils.saveChartAsPNG(lineChart, lineChartObject, 1920, 1080);
        } else if (true)    {
            int samples = 400;

            DefaultXYZDataset dataset;
            int max;
            {
                dataset = new DefaultXYZDataset();
                double[] xValues = new double[samples * samples];
                double[] yValues = new double[samples * samples];
                double[] zValues = new double[samples * samples];
                Map<Vec2i, AtomicInteger> missingPositionsCounter = new HashMap<>();
                positions.parallelStream()
                        .map(pos -> new Vec2i(
                                min(49 * samples / 100, max(-50 * samples / 100, pos.x / (1024 / (samples / 100)))),
                                min(49 * samples / 100, max(-50 * samples / 100, pos.z / (1024 / (samples / 100))))
                                ))
                        .forEach(pos -> missingPositionsCounter.computeIfAbsent(pos, p -> new AtomicInteger()).incrementAndGet());
                max = missingPositionsCounter.values().stream()
                        .map(AtomicInteger::get)
                        .max(Integer::compare)
                        .get();
                for (int x = samples - 1; x >= 0; x--)  {
                    for (int y = samples - 1; y >= 0; y--)  {
                        xValues[x * samples + y] = x;// - 50;
                        yValues[x * samples + y] = y;// - 50;
                        AtomicInteger i = missingPositionsCounter.get(new Vec2i(x - 50 * samples / 100, y - 50 * samples / 100));
                        zValues[x * samples + y] = i == null ? 0 : i.get();
                        if (false && i != null)  {
                            i.get();
                        }
                    }
                }
                dataset.addSeries("missing", new double[][]{xValues, yValues, zValues});
            }
            String[] labels = new String[samples];
            for (int i = samples - 1; i >= 0; i--)  {
                labels[i] = String.format("%dk", (i - 50 * samples / 100) * (1024 / (samples / 100)));
            }
            SymbolAxis xAxis = new SymbolAxis("x", labels);
            xAxis.setLowerMargin(0.0d);
            xAxis.setUpperMargin(0.0d);
            SymbolAxis yAxis = new SymbolAxis("y", labels);
            xAxis.setLowerMargin(0.0d);
            xAxis.setUpperMargin(0.0d);

            NumberAxis valueAxis1 = new NumberAxis("Marker");
            valueAxis1.setLowerMargin(0);
            valueAxis1.setUpperMargin(0);
            valueAxis1.setVisible(false);

            LookupPaintScale scale = new LookupPaintScale(0, max, Color.BLACK);
            {
                double step = (double) max / 255.0d;
                for (int i = 255; i >= 0; i--)  {
                    scale.add(step * i, new Color(i, 0, 0));
                }
            }
            
            PaintScaleLegend psl = new PaintScaleLegend(scale, new NumberAxis());
            psl.setPosition(RectangleEdge.RIGHT);
            psl.setAxisLocation(AxisLocation.TOP_OR_RIGHT);
            psl.setMargin(50.0, 20.0, 80.0, 0.0);

            XYPlot plot = new XYPlot(dataset, xAxis, yAxis, new XYBlockRenderer());
            ((XYBlockRenderer)plot.getRenderer()).setPaintScale(scale);
            // 2 optional lines, depending on your y-values
            plot.setRangeAxis(1, valueAxis1);
            plot.mapDatasetToRangeAxis(0, 1);

            JFreeChart chart = new JFreeChart(null, null, plot, false);
            chart.addSubtitle(psl);

            File heatmap = new File("missing_heatmap.png");
            ChartUtils.saveChartAsPNG(heatmap, chart, 1920, 1920);
        }
    }
}
