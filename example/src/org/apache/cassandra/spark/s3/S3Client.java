package org.apache.cassandra.spark.s3;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import org.apache.cassandra.spark.data.DataLayer;

public class S3Client
{
    private final AmazonS3 s3;
    private final String bucket;

    public S3Client(String region, String bucket)
    {
        this.bucket = bucket;
        final String accessKey = System.getenv("ACCESS_KEY");
        final String secretKey = System.getenv("SECRET_KEY");
        final AWSCredentials creds = accessKey != null && secretKey != null
                                     ? new BasicAWSCredentials(accessKey, secretKey)
                                     : new AnonymousAWSCredentials();
        this.s3 = AmazonS3ClientBuilder
                  .standard()
                  .withRegion(region)
                  .withCredentials(new AWSStaticCredentialsProvider(creds))
                  .build();
    }

    private static String prefix(String clusterName, String keyspace, String table, String dc)
    {
        return clusterName + "/" + keyspace + "/" + table + "/" + dc + "/";
    }

    private static String instanceKey(String clusterName, String keyspace,
                                      String table, String dc, String token)
    {
        return prefix(clusterName, keyspace, table, dc) + token + "/";
    }

    private static String fileKey(String clusterName, String keyspace, String table, String dc,
                                  String token, String fileName, DataLayer.FileType fileType)
    {
        return instanceKey(clusterName, keyspace, table, dc, token) + fileName + fileType.getFileSuffix();
    }

    /**
     * @return list of instances found in the S3 bucket directory for a given cluster/keyspace/table/DC.
     */
    public List<String> instances(String clusterName,
                                  String keyspace,
                                  String table,
                                  String dc)
    {
        final String prefix = prefix(clusterName, keyspace, table, dc);
        final ListObjectsRequest req = new ListObjectsRequest()
                                       .withPrefix(prefix)
                                       .withDelimiter("/")
                                       .withBucketName(this.bucket);
        return s3.listObjects(req).getCommonPrefixes().stream()
                 .map(m -> m.replaceFirst(prefix, ""))
                 .map(m -> m.replace("/", ""))
                 .collect(Collectors.toList());
    }

    /**
     * @return list of sstables found in the instance directory
     */
    public List<String> sstables(String clusterName,
                                 String keyspace,
                                 String table,
                                 String dc,
                                 String token)
    {
        final String prefix = instanceKey(clusterName, keyspace, table, dc, token);
        final ListObjectsRequest req = new ListObjectsRequest()
                                       .withPrefix(prefix)
                                       .withDelimiter("/")
                                       .withBucketName(this.bucket);
        return s3.listObjects(req).getObjectSummaries().stream()
                 .map(m -> m.getKey().replaceFirst(prefix, ""))
                 .filter(name -> name.endsWith("-" + DataLayer.FileType.DATA.getFileSuffix()))
                 .collect(Collectors.toList());
    }

    /**
     * @return true if file component exists.
     */
    public boolean exists(String clusterName,
                          String keyspace,
                          String table,
                          String dc,
                          String token,
                          String fileName,
                          DataLayer.FileType fileType)
    {
        return s3.doesObjectExist(this.bucket, fileKey(clusterName, keyspace, table, dc, token, fileName, fileType));
    }

    /**
     * @return open & return an InputStream on the SSTable file component.
     */
    public InputStream open(String clusterName,
                            String keyspace,
                            String table,
                            String dc,
                            String token,
                            String fileName,
                            DataLayer.FileType fileType)
    {
        // NOTE: for large-scale production use-cases a streaming/chunked InputStream will be required that handles backpressure gracefully
        // to prevent the bulk reader buffering too much Data.db bytes in memory and causing OOMs.
        return s3.getObject(this.bucket, fileKey(clusterName, keyspace, table, dc, token, fileName, fileType))
                 .getObjectContent();
    }
}
