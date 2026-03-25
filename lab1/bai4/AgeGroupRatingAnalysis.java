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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeGroupRatingAnalysis {

    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();
        private final Map<Integer, String> movieMap = new HashMap<>();
        private final Map<Integer, Integer> userAgeMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Load movies.txt and users.txt from distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length < 2) {
                throw new IOException("movies.txt and users.txt not found in Distributed Cache.");
            }

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);

            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                String fileName = path.getName();

                if (fileName.equals("movies.txt")) {
                    loadMovies(fs, path);
                } else if (fileName.equals("users.txt")) {
                    loadUsers(fs, path);
                }
            }
        }

        private void loadMovies(FileSystem fs, Path path) throws IOException {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(path)))) {

                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    int firstComma = line.indexOf(",");
                    int secondComma = line.indexOf(",", firstComma + 1);

                    if (firstComma == -1 || secondComma == -1) {
                        continue;
                    }

                    try {
                        int movieId = Integer.parseInt(line.substring(0, firstComma).trim());
                        String title = line.substring(firstComma + 1, secondComma).trim();
                        movieMap.put(movieId, title);
                    } catch (NumberFormatException e) {
                        // skip invalid line
                    }
                }
            }
        }

        private void loadUsers(FileSystem fs, Path path) throws IOException {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(path)))) {

                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    String[] parts = line.split(",");
                    if (parts.length < 3) continue;

                    try {
                        int userId = Integer.parseInt(parts[0].trim());
                        int age = Integer.parseInt(parts[2].trim());
                        userAgeMap.put(userId, age);
                    } catch (NumberFormatException e) {
                        // skip invalid line
                    }
                }
            }
        }

        private String getAgeGroup(int age) {
            if (age < 18) return "0-18";
            else if (age < 35) return "18-35";
            else if (age < 50) return "35-50";
            else return "50+";
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                // Get age for this userId
                Integer age = userAgeMap.get(userId);
                if (age == null) {
                    return; // User not found
                }

                // Get movie title
                String title = movieMap.get(movieId);
                if (title == null) {
                    return; // Movie not found
                }

                // Determine age group
                String ageGroup = getAgeGroup(age);

                // Emit: movieId -> "title|rating|ageGroup"
                outKey.set(movieId);
                outValue.set(title + "|" + rating + "|" + ageGroup);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                // skip invalid line
            }
        }
    }

    public static class RatingReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private final DecimalFormat df = new DecimalFormat("0.00");

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double[] groupSum = new double[4]; // 0-18, 18-35, 35-50, 50+
            int[] groupCount = new int[4];
            String movieTitle = "";

            // Initialize
            for (int i = 0; i < 4; i++) {
                groupSum[i] = 0.0;
                groupCount[i] = 0;
            }

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                if (parts.length < 3) continue;

                movieTitle = parts[0];
                double rating = Double.parseDouble(parts[1]);
                String ageGroup = parts[2];

                int groupIdx = 0;
                if (ageGroup.equals("0-18")) groupIdx = 0;
                else if (ageGroup.equals("18-35")) groupIdx = 1;
                else if (ageGroup.equals("35-50")) groupIdx = 2;
                else if (ageGroup.equals("50+")) groupIdx = 3;

                groupSum[groupIdx] += rating;
                groupCount[groupIdx]++;
            }

            if (movieTitle.isEmpty()) return;

            // Calculate averages
            double[] groupAvg = new double[4];
            for (int i = 0; i < 4; i++) {
                groupAvg[i] = groupCount[i] > 0 ? groupSum[i] / groupCount[i] : 0.0;
            }

            String[] ageGroupLabels = { "0-18", "18-35", "35-50", "50+" };
            StringBuilder sb = new StringBuilder();
            sb.append(movieTitle);

            for (int i = 0; i < 4; i++) {
                sb.append("\t").append(ageGroupLabels[i]).append(": ");
                if (groupCount[i] == 0) {
                    sb.append("NA");
                } else {
                    sb.append(df.format(groupAvg[i]));
                }
            }

            context.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar AgeGroupRatingAnalysis.jar AgeGroupRatingAnalysis <input_ratings> <output> <movies_file> <users_file>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        
        // Enable local mode execution
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        
        Job job = Job.getInstance(conf, "Age Group Based Rating Analysis");

        job.setJarByClass(AgeGroupRatingAnalysis.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
