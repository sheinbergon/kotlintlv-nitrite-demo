package org.sheinbergon.nitrite;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.val;
import org.apache.commons.text.RandomStringGenerator;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.mapper.JacksonMapper;
import org.dizitart.no2.mapper.NitriteMapper;
import org.dizitart.no2.objects.ObjectRepository;
import org.dizitart.no2.objects.filters.ObjectFilters;
import org.jetbrains.annotations.Nullable;
import org.jooq.lambda.Unchecked;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class QueryBenchmark {

    private volatile static String TMP = System.getProperty("java.io.tmpdir");

    private final static int FORKS = 1;

    private final static int WARMUPS = 3;

    private final static int ITERATIONS = 9;

    private final static int MILLISECONDS = 1500;

    private final static int RANDOM_STRING_LENGTH = 64;

    private final static RandomStringGenerator GENERATOR;

    private final static Random RANDOM;

    private final static String CREATE_TABLE_STATEMENT =
            "CREATE TABLE IF NOT EXISTS arbitrary ("
                    + "  id INTEGER PRIMARY KEY,"
                    + "  text TEXT NOT NULL,"
                    + "  number1 REAL NOT NULL,"
                    + "  number2 REAL NOT NULL,"
                    + "  index1 INTEGER NOT NULL,"
                    + "  flag1 INTEGER NOT NULL,"
                    + "  flag2 INTEGER NOT NULL);";

    private final static String CREATE_INDEX1_STATEMENT =
            "CREATE INDEX index1_idx ON arbitrary(index1)";

    private final static String INSERT_TABLE_STATEMENT = "INSERT INTO arbitrary(id,text,number1,number2,index1,flag1,flag2) " +
            " VALUES (?,?,?,?,?,?,?)";

    private final static String SELECT_INDEX1_STATEMENT =
            "SELECT * FROM arbitrary WHERE index1=?";

    static {
        try {
            RANDOM = SecureRandom.getInstance("NativePRNGNonBlocking");
            GENERATOR = new RandomStringGenerator.Builder().usingRandom(RANDOM::nextInt).build();
        } catch (SecurityException | NoSuchAlgorithmException x) {
            throw new RuntimeException(x);
        }
    }

    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor
    public enum Database {
        NITRITE_MEMORY(null, new JacksonMapper()),
        NITRITE_OPTIMIZED(null, ArbitraryDataMapper.INSTANCE),
        NITRITE_FILE(String.format("%s/nitrite.db", TMP), new JacksonMapper()),
        SQLITE_MEMORY(":memory:", null),
        SQLITE_FILE(String.format("%s/sqlite.db", TMP), null);

        @Nullable
        private final String path;

        @Nullable
        private final NitriteMapper mapper;

    }

    private AtomicInteger sequence = null;
    private Nitrite nitrite = null;
    private Connection sqliteConnection = null;
    private PreparedStatement sqliteQuery = null;
    private ObjectRepository<ArbitraryData> repository = null;

    @Param({"SQLITE_FILE", "SQLITE_MEMORY", "NITRITE_FILE", "NITRITE_MEMORY", "NITRITE_OPTIMIZED"})
    private Database database;

    @Param({"10000","20000","50000"})
    private int dataSetSize;

    @Param({"100","500","1000","2000"})
    private int indexDispersion;

    private ArbitraryData randomDatum() {
        return new ArbitraryData()
                .id(sequence.incrementAndGet())
                .flag1(RANDOM.nextBoolean())
                .flag2(RANDOM.nextBoolean())
                .number1(RANDOM.nextDouble())
                .number2(RANDOM.nextDouble())
                .index1(RANDOM.nextInt(indexDispersion))
                .text(GENERATOR.generate(RANDOM_STRING_LENGTH));
    }

    private ArbitraryData[] randomData() {
        sequence = new AtomicInteger(0);
        return IntStream.range(0, dataSetSize)
                .mapToObj(index -> randomDatum())
                .toArray(ArbitraryData[]::new);
    }

    private void insertDataIntoNitrite(ArbitraryData[] data) {
        repository.insert(data);
    }

    private void insertDataIntoSQLite(ArbitraryData[] data) throws SQLException {
        sqliteConnection.setAutoCommit(false);
        val statement = sqliteConnection.prepareStatement(INSERT_TABLE_STATEMENT);
        for (ArbitraryData datum : data) {
            statement.setInt(1, datum.id());
            statement.setString(2, datum.text());
            statement.setDouble(3, datum.number1());
            statement.setDouble(4, datum.number2());
            statement.setInt(5, datum.index1());
            statement.setBoolean(6, datum.flag1());
            statement.setBoolean(7, datum.flag2());
            statement.addBatch();
        }
        statement.executeBatch();
        sqliteConnection.setAutoCommit(true);
    }

    private void setupSQLite(@NonNull String path) throws SQLException, IOException {
        Files.deleteIfExists(Path.of(path));
        val jdbcUrl = String.format("jdbc:sqlite:%s", path);
        sqliteConnection = DriverManager.getConnection(jdbcUrl);
        sqliteConnection.createStatement().execute(CREATE_TABLE_STATEMENT);
        sqliteConnection.createStatement().execute(CREATE_INDEX1_STATEMENT);
        sqliteQuery = sqliteConnection.prepareStatement(SELECT_INDEX1_STATEMENT);
    }

    private void tearDownSQLite() {
        Optional.ofNullable(sqliteQuery)
                .ifPresent(Unchecked.consumer(Statement::close));
        Optional.ofNullable(sqliteConnection)
                .ifPresent(Unchecked.consumer(Connection::close));
    }

    private void setupNitrite(@NonNull NitriteMapper mapper, @Nullable String path) {
        nitrite = Optional.ofNullable(path).map(Unchecked.function(p -> {
            deleteFile(p);
            return Nitrite.builder().nitriteMapper(mapper).filePath(p).openOrCreate();
        })).orElseGet(() -> Nitrite.builder().nitriteMapper(mapper).openOrCreate());
        repository = nitrite.getRepository(ArbitraryData.class);
        repository.createIndex("index1", IndexOptions.indexOptions(IndexType.NonUnique));
    }

    private void deleteFile(@NonNull String path) throws IOException {
        Files.deleteIfExists(Path.of(path));
    }

    private void tearDownNitrite() {
        Optional.ofNullable(nitrite)
                .ifPresent(Nitrite::close);
    }

    private Collection<ArbitraryData> inquireNitrite(int indexValue) {
        return repository.find(ObjectFilters.eq("index1", indexValue)).toList();
    }

    private Collection<ArbitraryData> inquireSQLite(int indexValue) throws SQLException {
        sqliteQuery.clearParameters();
        sqliteQuery.setInt(1, indexValue);
        val result = sqliteQuery.executeQuery();
        val data = new ArrayList<ArbitraryData>(indexDispersion * 10);
        while (result.next()) {
            val datum = new ArbitraryData()
                    .id(result.getInt("id"))
                    .text(result.getString("text"))
                    .number1(result.getDouble("number1"))
                    .number2(result.getDouble("number2"))
                    .index1(result.getInt("index1"))
                    .flag1(result.getBoolean("flag1"))
                    .flag2(result.getBoolean("flag2"));
            data.add(datum);
        }
        return data;
    }


    @Setup
    public void setup() throws Exception {
        val data = randomData();
        switch (database) {
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                setupSQLite(database.path);
                insertDataIntoSQLite(data);
                break;
            case NITRITE_FILE:
            case NITRITE_MEMORY:
            case NITRITE_OPTIMIZED:
                setupNitrite(database.mapper, database.path);
                insertDataIntoNitrite(data);
                break;
        }
    }

    @TearDown
    public void tearDown() {
        switch (database) {
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                tearDownSQLite();
                break;
            case NITRITE_FILE:
            case NITRITE_MEMORY:
            case NITRITE_OPTIMIZED:
                tearDownNitrite();
                break;
        }
    }

    @Benchmark
    @Fork(value = FORKS, jvmArgsAppend = {
            "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
            "-Xmx8192m",
            "-Xmn6144m"})
    @Warmup(iterations = WARMUPS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = ITERATIONS, time = MILLISECONDS, timeUnit = TimeUnit.MILLISECONDS)
    public void inquire() throws SQLException {
        val indexValue = RANDOM.nextInt(indexDispersion);
        Collection<ArbitraryData> results = null;
        switch (database) {
            case NITRITE_FILE:
            case NITRITE_MEMORY:
            case NITRITE_OPTIMIZED:
                results = inquireNitrite(indexValue);
                break;
            case SQLITE_FILE:
            case SQLITE_MEMORY:
                results = inquireSQLite(indexValue);
                break;
        }
        Optional.ofNullable(results)
                .ifPresent(data -> data.forEach(r -> {
                }));
    }
}
