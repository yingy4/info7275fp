package bloomfiltering;

import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import com.google.common.base.Charsets;

public class LocationFunnel implements Funnel<Location> {
    @Override
    public void funnel(Location location, Sink sink) {
        sink.putString(location.country, Charsets.UTF_8)
                .putString(location.type, Charsets.UTF_8);
    }
}
