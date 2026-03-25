import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingAnalysis {

    public static class RatingMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private final IntWritable outKey = new IntWritable();
        private final DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            try {
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                outKey.set(movieId);
                outValue.set(rating);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                // skip invalid line
            }
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, DoubleWritable, Text, NullWritable> {
        private final Map<Integer, String> movieMap = new HashMap<>();
        private final DecimalFormat df = new DecimalFormat("0.00");

        private String maxMovie = "";
        private double maxRating = Double.NEGATIVE_INFINITY;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("movies.txt not found in Distributed Cache.");
            }

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                if (!path.getName().equals("movies.txt")) continue;

                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(path)))) {

                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;

                        int firstComma = line.indexOf(",");
                        int lastComma = line.lastIndexOf(",");

                        if (firstComma == -1 || lastComma == -1 || firstComma == lastComma) {
                            continue;
                        }

                        try {
                            int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
                            String title = line.substring(firstComma + 1, lastComma).trim();
                            movieMap.put(movieId, title);
                        } catch (NumberFormatException e) {
                            // skip invalid line
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count == 0) return;

            double avg = sum / count;
            int movieId = key.get();
            String title = movieMap.getOrDefault(movieId, "UnknownMovie(" + movieId + ")");

            String output = title + "\tAverage rating: " + df.format(avg) +
                            " (Total ratings: " + count + ")";
            context.write(new Text(output), NullWritable.get());

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = title;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                String result = maxMovie +
                        " is the highest rated movie with an average rating of " +
                        df.format(maxRating) +
                        " among movies with at least 5 ratings.";
                context.write(new Text(result), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: hadoop jar MovieRatingAnalysis.jar MovieRatingAnalysis <input_ratings> <output> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        
        Job job = Job.getInstance(conf, "Movie Average Rating and Count");

        job.setJarByClass(MovieRatingAnalysis.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}