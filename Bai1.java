// Hadoop MapReduce: Tính điểm trung bình và tổng số lượt đánh giá cho mỗi phim, xuất MovieTitle
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.HashMap;

public class Bai1 {

    // đọc 2 file ratings_1 và ratings_2 để bóc tách lấy mã phim và điểm số
    public static class MovieRatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String movieId = fields[1].trim();
                try {
                    float rating = Float.parseFloat(fields[2].trim());
                    context.write(new Text(movieId), new FloatWritable(rating)); // nhả ra cặp Key-Value
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
    }

    public static class MovieRatingReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private HashMap<String, String> movieIdToTitle = new HashMap<>();
        private String maxMovie = "";
        private float maxRating = 0;
        private int maxCount = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI cacheFile : cacheFiles) {
                    String path = cacheFile.getPath();
                    if (path.contains("movies.txt")) {
                        BufferedReader reader = new BufferedReader(new FileReader(path));
                        String line;
                        while ((line = reader.readLine()) != null) {
                            String[] parts = line.split(",", 3);
                            if (parts.length >= 2) {
                                String movieId = parts[0].trim();
                                String title = parts[1].trim();
                                movieIdToTitle.put(movieId, title);
                            }
                        }
                        reader.close();
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float avg = sum / count;
            String movieTitle = movieIdToTitle.getOrDefault(key.toString(), key.toString());
            context.write(new Text(movieTitle), new Text(String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count)));
            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieTitle;
                maxCount = count;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(new Text("\n" + maxMovie), new Text(String.format("is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.", maxRating)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai1 <ratings_1.txt> <ratings_2.txt> <movies.txt> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Average Rating");
        job.setJarByClass(Bai1.class);
        job.setMapperClass(MovieRatingMapper.class);
        job.setReducerClass(MovieRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        // Thêm movies.txt vào DistributedCache
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 