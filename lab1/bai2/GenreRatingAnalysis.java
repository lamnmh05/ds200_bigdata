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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenreRatingAnalysis {

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text outKey = new Text();
        private final DoubleWritable outValue = new DoubleWritable();
        private final Map<Integer, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load movies.txt from distributed cache
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
                            String genres = line.substring(lastComma + 1).trim();
                            movieMap.put(movieId, genres);
                        } catch (NumberFormatException e) {
                            // skip invalid line
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                // Get genres for this movieId
                String genresStr = movieMap.get(movieId);
                if (genresStr == null || genresStr.isEmpty()) {
                    return; // Movie not found in movies.txt
                }

                // Split genres by "|"
                String[] genres = genresStr.split("\\|");

                // Emit each genre with the rating
                for (String genre : genres) {
                    genre = genre.trim();
                    if (!genre.isEmpty()) {
                        outKey.set(genre);
                        outValue.set(rating);
                        context.write(outKey, outValue);
                    }
                }

            } catch (NumberFormatException e) {
                // skip invalid line
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
        private final DecimalFormat df = new DecimalFormat("0.00");

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count == 0) return;

            double avg = sum / count;
            String genre = key.toString();

            String output = genre + "\tAvg: " + df.format(avg) + ", Count: " + count;
            context.write(new Text(output), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: hadoop jar GenreRatingAnalysis.jar GenreRatingAnalysis <input_ratings> <output> <movies_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        
        // Enable local mode execution (no YARN/containers needed)
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        
        Job job = Job.getInstance(conf, "Genre Average Rating Analysis");

        job.setJarByClass(GenreRatingAnalysis.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
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
