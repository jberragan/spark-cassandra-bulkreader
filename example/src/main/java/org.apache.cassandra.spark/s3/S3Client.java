package org.apache.cassandra.spark.s3;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringUtils;
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
     * @return list of sstables found in the instance directory and size in bytes for all file types
     */
    public Map<String, Map<DataLayer.FileType, Long>> sstables(String clusterName,
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
        final List<S3ObjectSummary> objects = s3.listObjects(req).getObjectSummaries();
        final Map<String, Map<DataLayer.FileType, Long>> sizes = new HashMap<>();
        for (final S3ObjectSummary object : objects)
        {
            final String key = object.getKey().replaceFirst(prefix, "");
            if (StringUtils.isNullOrEmpty(key)) {
                continue;
            }
            final DataLayer.FileType fileType = DataLayer.FileType.fromExtension(key.substring(key.lastIndexOf("-") + 1));
            final String name = key.replace(fileType.getFileSuffix(), "");
            sizes.putIfAbsent(name, new HashMap<>(8));
            sizes.get(name).put(fileType, object.getSize());
        }
        return sizes;
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
     * @return return byte[] for SSTable file component byte range.
     */
    public byte[] read(String clusterName,
                       String keyspace,
                       String table,
                       String dc,
                       String token,
                       String fileName,
                       DataLayer.FileType fileType,
                       long start, long end) throws IOException
    {
        final GetObjectRequest req = new GetObjectRequest(this.bucket, fileKey(clusterName, keyspace, table, dc, token, fileName, fileType))
                                     .withRange(start, end);
        return IOUtils.toByteArray(s3.getObject(req).getObjectContent());
    }
}
