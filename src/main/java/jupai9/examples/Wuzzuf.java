package jupai9.examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.knowm.xchart.*;
import org.knowm.xchart.demo.charts.ExampleChart;
import org.knowm.xchart.demo.charts.pie.PieChart02;
import org.knowm.xchart.style.Styler;


import java.awt.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.*;
import java.util.List;
import java.text.ParseException; import java.util.ArrayList; import java.util.Collections; import java.util.Comparator; import java.util.HashMap; import java.util.LinkedHashMap; import java.util.List; import java.util.Map.Entry; import java.util.Set; import java.util.TreeMap;


public class Wuzzuf {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Our App").master ("local[6]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
         Dataset<Row> df = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");

        // Print Schema to see column names, types and other metadata
        df.show (10);
        df.printSchema ();
        System.out.println (df.count ());

        df = df.filter("YearsExp != 'null Yrs of Exp'");
        //System.out.println (wuzzefDF.count ());


        df = df.distinct();
        //System.out.println (wuzzefDF.count ());

        df.show (20);



        Dataset<Row> skills = df.select("Skills");
        List<Row> skillsRows = skills.collectAsList();

        ArrayList<String> skills_list = new ArrayList<String>(); // Create an ArrayList object

        for(int i = 0; i < skillsRows.size();i++) {
            String s = (String) skillsRows.get(i).get(0);
            String[] res = s.split("[,]", 0);
            for(String myStr: res) {
                // System.out.println(myStr);

                skills_list.add(myStr);
            }
            //skills_list.add();
            //max.set(j,0);
        }

        skillsCount(skills_list);


        Dataset<Row> compDf = df.groupBy("Company").count().filter("count >= 10");
        Dataset<Row> topComp = compDf.sort(compDf.col("count").desc());
        topComp.show(5);


        PieChart topCompChart = pieChart(topComp.collectAsList().subList(0,20),"Top 20 Companies");
        new SwingWrapper<PieChart>(topCompChart).displayChart();


        Dataset<Row> titDf = df.groupBy("Title").count().filter("count >= 10");
        Dataset<Row> topTit = titDf.sort(titDf.col("count").desc());
        topTit.show(5);



        CategoryChart topTitChart = barChart(topTit.collectAsList().subList(0,5),"Top 20 Titles");
        new SwingWrapper<CategoryChart>(topTitChart).displayChart();

        Dataset<Row> locDf = df.groupBy("Location").count().filter("count >= 10");
        Dataset<Row> topLoc = locDf.sort(locDf.col("count").desc());
        topLoc.show(5);



        CategoryChart topLocChart = barChart(topLoc.collectAsList().subList(0,8),"Top 20 Locations");
        new SwingWrapper<CategoryChart>(topLocChart).displayChart();




    }

    public static void skillsCount(ArrayList<String> list)
    {
        // hashmap to store the frequency of element
        Map<String, Integer> hm = new HashMap<String, Integer>();

        for (String i : list) {
            Integer j = hm.get(i);
            hm.put(i, (j == null) ? 1 : j + 1);
        }


        // displaying the occurrence of elements in the arraylist

        for (Map.Entry<String, Integer> val : hm.entrySet()) {
             System.out.println("Element " +  val.getKey() + " "
                    + "occurs"
                   + ": " + val.getValue() + " times");

        }
    }

    public static CategoryChart barChart(List<Row> rows, String name) {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(name).xAxisTitle("Name").yAxisTitle("Times").build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        String a[] = new String[rows.size()];
        List x = Arrays.asList(a);

        Long b[] = new Long[rows.size()];
        List y = Arrays.asList(b);

        // Series
        //chart.addSeries("test 1", Arrays.asList(new Integer[] { 0, 1, 2, 3, 4 }), Arrays.asList(new Integer[] { 4, 5, 9, 6, 5 }));
        for (int i = 0;i< rows.size();i++)
        {
            x.set(i,rows.get(i).get(0));
            y.set(i,rows.get(i).get(1));

        }

        chart.addSeries("Skill", x, y);

        return chart;
    }

    public static PieChart pieChart(List<Row> rows, String name) {

        // Create Chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(name).build();

        // Customize Chart
        Color[] sliceColors = new Color[] { new Color(224, 68, 14), new Color(230, 105, 62), new Color(236, 143, 110), new Color(243, 180, 159), new Color(246, 199, 182) };
        chart.getStyler().setSeriesColors(sliceColors);

        for (int i = 0;i< rows.size();i++)
        {
            chart.addSeries((String)rows.get(i).get(0),(Long)rows.get(i).get(1));
        }


        return chart;
    }




}
