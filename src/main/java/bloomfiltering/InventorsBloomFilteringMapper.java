package bloomfiltering;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InventorsBloomFilteringMapper extends Mapper<Object, Text, Text, NullWritable> {

    private Funnel<Location> fl = new LocationFunnel();
    private BloomFilter<Location> locationFilter = BloomFilter.create(fl, 1, 0.1);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Location l = new Location("US","residence");
        locationFilter.put(l);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] sline = line.split(",");
        Location myLocation = new Location(sline[6].trim(),sline[8].trim());
        if (locationFilter.mightContain(myLocation)) {
            context.write(value, NullWritable.get());
        }
    }

}
