package net.gutefrage.tsdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;

import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proof of Concept This OpenTSDB Plugin publishes data to TAFE.
 * */
public class SkylinePublisher extends RTPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(SkylinePublisher.class);

    @Override
    public void initialize(final TSDB tsdb) {
        LOG.info("init SkylinePublisher");
    }

    @Override
    public Deferred<Object> shutdown() {
        return new Deferred<Object>();
    }

    @Override
    public String version() {
        return "2.0.1";
    }

    @Override
    public void collectStats(final StatsCollector collector) {
    }

    @Override
    public Deferred<Object> publishDataPoint(final String metric,
            final long timestamp, final long value, final Map<String, String> tags,
            final byte[] tsuid) {

        sendSocket(makeMetricName(metric, tags), timestamp, value);

        return new Deferred<Object>();
    }

    @Override
    public Deferred<Object> publishDataPoint(final String metric,
            final long timestamp, final double value, final Map<String, String> tags,
            final byte[] tsuid) {

        sendSocket(makeMetricName(metric, tags), timestamp, value);

        return new Deferred<Object>();
    }

    /*
     * a skyline metric name will be in the format of
     * <metric>.<tag1_key>_<tag1_value>.<tag2_key>_<tag2_value> Do this instead of adding a
     * dependency on a json library or what not for now.
     */
    private String makeMetricName(String metric, Map<String, String> tags) {
        String metricName = metric;
        SortedSet<String> keys = new TreeSet<String>(tags.keySet());
        for (String key : keys) {
            metricName = metricName.concat("|" + key + "|" + tags.get(key));
        }
        return metricName;
    }

    //Sends the data to the skyline server
    private void sendSocket(String skylineMetricName, final long timestamp, final double value) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpPost httpPost = new HttpPost("http://api.tellapart.com:4123/xraym");

            // Request parameters and other properties.
            List<NameValuePair> params = new ArrayList<NameValuePair>();
            params.add(new BasicNameValuePair("metric", skylineMetricName));
            httpPost.setEntity(new UrlEncodedFormEntity(params));

            // Execute and get the response.
            CloseableHttpResponse response = null;
            try {
                response = httpclient.execute(httpPost);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    throw new IOException("Non 200 status code");
                }
                LOG.info("Sent metric: " + skylineMetricName + " successfully");
                EntityUtils.consume(response.getEntity());
            } finally {
                response.close();
            }

        } catch (IOException e) {
            LOG.error("IOException while sending metric: " + skylineMetricName);
            e.printStackTrace();
        } finally {
            try {
                httpclient.close();
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
        }

    }
}
