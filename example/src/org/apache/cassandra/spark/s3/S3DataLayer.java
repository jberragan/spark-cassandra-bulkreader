package org.apache.cassandra.spark.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.cassandra.spark.data.CqlSchema;
import org.apache.cassandra.spark.data.PartitionedDataLayer;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.fourzero.complex.CqlUdt;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.ConsistencyLevel;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import org.apache.cassandra.spark.reader.CassandraBridge;
import org.apache.cassandra.spark.reader.fourzero.FourZeroSchemaBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class S3DataLayer extends PartitionedDataLayer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataLayer.class);
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(32,
                                                                                         new ThreadFactoryBuilder()
                                                                                         .setNameFormat("cassandra-bulkreader-%d")
                                                                                         .setDaemon(true)
                                                                                         .build()
    );

    private String clusterName, keyspace, table, tableCreateStmt, s3Region, s3Bucket;
    private CassandraRing ring;
    private TokenPartitioner partitioner;
    private CqlSchema schema;
    private S3Client s3Client = null;

    public S3DataLayer(@Nullable ConsistencyLevel consistencyLevel,
                       @NotNull final String clusterName,
                       @NotNull final String keyspace,
                       @NotNull final String table,
                       @NotNull final String tableCreateStmt,
                       @NotNull String dc,
                       @NotNull String s3Region,
                       @NotNull String s3Bucket,
                       final int defaultParallelism,
                       final int numCores)
    {
        super(consistencyLevel, dc);
        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.table = table;
        this.tableCreateStmt = tableCreateStmt;
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        init();

        // list Cassandra instances in S3 bucket
        final List<CassandraInstance> instances = s3Client
                                                  .instances(clusterName, keyspace, table, dc)
                                                  .stream()
                                                  .map(token -> new CassandraInstance(token, token, dc))
                                                  .collect(Collectors.toList());

        // build CassandraRing and TokenPartitioner
        final Partitioner partitioner = Partitioner.Murmur3Partitioner;
        final ReplicationFactor rf = new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, ImmutableMap.of("DC1", 3, "DC2", 3));
        this.ring = new CassandraRing(Partitioner.Murmur3Partitioner, keyspace, rf, instances);
        this.partitioner = new TokenPartitioner(ring, defaultParallelism, numCores);

        // parse schema from tableCreateStmt
        final Set<String> udtStmts = Collections.emptySet(); // any udt definitions used in the table schema
        this.schema = new FourZeroSchemaBuilder(tableCreateStmt, keyspace, rf, partitioner, udtStmts).build();
    }

    // for deserialization
    private S3DataLayer(@Nullable ConsistencyLevel consistencyLevel,
                        @NotNull final String clusterName,
                        @NotNull final String keyspace,
                        @NotNull final String table,
                        @NotNull final String tableCreateStmt,
                        @Nullable String dc,
                        @NotNull String s3Region,
                        @NotNull String s3Bucket,
                        @NotNull final TokenPartitioner partitioner,
                        @NotNull final CassandraRing ring,
                        @NotNull final CqlSchema schema)
    {
        super(consistencyLevel, dc);
        this.clusterName = clusterName;
        this.keyspace = keyspace;
        this.table = table;
        this.tableCreateStmt = tableCreateStmt;
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.partitioner = partitioner;
        this.ring = ring;
        this.schema = schema;
        init();
    }

    private void init()
    {
        if (s3Client == null)
        {
            this.s3Client = new S3Client(s3Region, s3Bucket);
        }
    }

    @Override
    public CassandraBridge.CassandraVersion version()
    {
        return CassandraBridge.CassandraVersion.THREEZERO;
    }

    @Override
    public CqlSchema cqlSchema()
    {
        return this.schema;
    }

    @Override
    public CompletableFuture<Stream<SSTable>> listInstance(int partitionId,
                                                           @NotNull Range<BigInteger> range,
                                                           @NotNull CassandraInstance instance)
    {
        // list all Data.db files in instance S3 directory
        // and create an S3SSTable object per Data.db file
        return CompletableFuture.supplyAsync(
        () ->
        s3Client.sstables(clusterName, keyspace, table, instance.dataCenter(), instance.nodeName())
                .stream()
                .peek(name -> LOGGER.info("Opening SSTable token={} fileName={}", instance.nodeName(), name))
                .map(name -> new S3SSTable(instance.nodeName(), name.replace(FileType.DATA.getFileSuffix(), "")))
        , EXECUTOR_SERVICE);
    }

    @Override
    public CassandraRing ring()
    {
        return this.ring;
    }

    @Override
    public TokenPartitioner tokenPartitioner()
    {
        return this.partitioner;
    }

    @Override
    protected ExecutorService executorService()
    {
        return S3DataLayer.EXECUTOR_SERVICE;
    }

    public class S3SSTable extends SSTable
    {
        private final String token;
        private final String fileName;

        public S3SSTable(String token,
                         String fileName)
        {
            this.token = token;
            this.fileName = fileName;
        }

        @Nullable
        protected InputStream openInputStream(FileType fileType)
        {
            // open an InputStream on the SSTable file component
            return s3Client.open(clusterName, keyspace, table, dc, token, fileName, fileType);
        }

        public boolean isMissing(FileType fileType)
        {
            return !s3Client.exists(clusterName, keyspace, table, dc, token, fileName, fileType);
        }

        public String getDataFileName()
        {
            return fileName;
        }

        public int hashCode()
        {
            return Objects.hash(token, fileName);
        }

        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (obj == this)
            {
                return true;
            }
            if (obj.getClass() != getClass())
            {
                return false;
            }

            final S3SSTable rhs = (S3SSTable) obj;
            return token.equals(rhs.token)
                   && fileName.equals(rhs.fileName);
        }
    }

    // jdk serialization

    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK deserialization");
        this.clusterName = in.readUTF();
        this.keyspace = in.readUTF();
        this.table = in.readUTF();
        this.tableCreateStmt = in.readUTF();
        this.s3Region = in.readUTF();
        this.s3Bucket = in.readUTF();
        this.schema = (CqlSchema) in.readObject();
        this.partitioner = (TokenPartitioner) in.readObject();
        this.ring = (CassandraRing) in.readObject();
        this.init();
    }

    private void writeObject(final ObjectOutputStream out) throws IOException, ClassNotFoundException
    {
        LOGGER.warn("Falling back to JDK serialization");
        out.writeUTF(this.clusterName);
        out.writeUTF(this.keyspace);
        out.writeUTF(this.table);
        out.writeUTF(this.tableCreateStmt);
        out.writeUTF(this.s3Region);
        out.writeUTF(this.s3Bucket);
        out.writeObject(this.schema);
        out.writeObject(this.partitioner);
        out.writeObject(this.ring);
    }

    // kryo serialization

    public static class Serializer extends com.esotericsoftware.kryo.Serializer<S3DataLayer>
    {
        @Override
        public void write(final Kryo kryo, final Output out, final S3DataLayer obj)
        {
            LOGGER.info("Serializing with Kryo");
            kryo.writeObject(out, obj.consistencyLevel);
            out.writeString(obj.clusterName);
            out.writeString(obj.keyspace);
            out.writeString(obj.table);
            out.writeString(obj.tableCreateStmt);
            out.writeString(obj.dc);
            out.writeString(obj.s3Region);
            out.writeString(obj.s3Bucket);
            kryo.writeObject(out, obj.partitioner);
            kryo.writeObject(out, obj.ring);
            kryo.writeObject(out, obj.schema);
        }

        @Override
        public S3DataLayer read(final Kryo kryo, final Input in, final Class<S3DataLayer> type)
        {
            LOGGER.info("Deserializing with Kryo");
            return new S3DataLayer(
            kryo.readObject(in, ConsistencyLevel.class),
            in.readString(),
            in.readString(),
            in.readString(),
            in.readString(),
            in.readString(),
            in.readString(),
            in.readString(),
            kryo.readObject(in, TokenPartitioner.class),
            kryo.readObject(in, CassandraRing.class),
            kryo.readObject(in, CqlSchema.class)
            );
        }
    }
}
