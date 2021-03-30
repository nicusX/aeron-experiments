# Aeron Archive Samples

The scripts in this directory can launch the sample applications for the [Aeron Archive](https://github.com/real-logic/aeron/tree/master/aeron-archive) service with the code samples
[here](https://github.com/real-logic/aeron/tree/master/aeron-samples/src/main/java/io/aeron/samples/archive) and
systems tests [here](https://github.com/real-logic/aeron/tree/master/aeron-system-tests/src/test/java/io/aeron/archive).

## Embedded Throughput Samples

Suitable for measuring IO performance of your system. The samples will default to creating
an archive directory on the temporary file system. It is recommended that a properties file is provided
by passing it as argument to the script. The `aeron.archive.dir` property should be located on fast storage.

### Recording Throughput

`embedded-recording-throughput`: Will record a number of messages using the `SampleConfiguration` properties then 
ask if the test should be repeated. Doing it multiple times allows the system to warm up.

It is worth trying different levels of write synchronisation for durability.

- `aeron.archive.file.sync.level=0`: for normal writes to the OS page cache for background writing to disk.
- `aeron.archive.file.sync.level=1`: for forcing the dirty data pages to disk. 
- `aeron.archive.file.sync.level=2`: for forcing the dirty data pages and file metadata to disk. 

### Replay Throughput

`embedded-replay-throughput`: Will record a number of messages using the `SampleConfiguration` properties then
replay them on a new stream. The test will ask to repeat the replay of record message and doing it multiple times
allows for the system to warm up.

It is worth playing with different messages lengths and threading configurations.

## Basic Publication and Subscription to an archived stream

1. Start the archiving media driver in its own console.

```
    $ archiving-media-driver <config properties file>
```

2. Start the publisher with its recorded publication in its own console.

```
    $ recorded-basic-publisher <config properties file>
```
    

3. Start a normal subscriber so that the publication connects and can record.

```
    $ cd ..
    $ basic-subscriber <config properties file>
```

4. Start a subscriber that requests replay of the recorded stream.

```
    $ replay-basic-subscriber <config properties file>
```

5. Optionally run AeronStat and observe status, look out for the `rec-pos` counter for the recorded stream. 
  
```
    $ aeron-stat
```

6. Check for errors.

```
    $ error-stat
```
 
