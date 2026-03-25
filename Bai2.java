// Hadoop MapReduce: Phân tích đánh giá theo thể loại (Genres)
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.HashSet;

public class Bai2 {
    // Mapper: Đọc ratings, join với movies.txt để lấy genres, nhả (Genre, Rating)
    public static class GenreRatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private HashMap<String, String> movieIdToGenres = new HashMap<>();
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
                            if (parts.length == 3) {
                                String movieId = parts[0].trim();
                                String genres = parts[2].trim();
                                movieIdToGenres.put(movieId, genres);
                            }
                        }
                        reader.close();
                    }
                }
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) {
                String movieId = fields[1].trim();
                try {
                    float rating = Float.parseFloat(fields[2].trim());
                    String genres = movieIdToGenres.get(movieId);
                    if (genres != null) {
                        for (String genre : genres.split("\\|")) {
                            context.write(new Text(genre.trim()), new FloatWritable(rating));
                        }
                    }
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
    }
    // Reducer: Tính điểm trung bình và tổng số lượt đánh giá cho từng thể loại
    public static class GenreRatingReducer extends Reducer<Text, FloatWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }
            float avg = sum / count;
            context.write(key, new Text(String.format("AverageRating: %.2f (TotalRatings: %d)", avg, count)));
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai2 <ratings_1.txt> <ratings_2.txt> <movies.txt> <output>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis");
        job.setJarByClass(Bai2.class);
        job.setMapperClass(GenreRatingMapper.class);
        job.setReducerClass(GenreRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.addCacheFile(new Path(args[2]).toUri()); // movies.txt
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
