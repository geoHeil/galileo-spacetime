[![Galileo Spacetime](docs/galileo-spacetime.png)](http://galileo.cs.colostate.edu/)
=======

Galileo Spacetime is a high-throughput distributed file system for spatio-temporal multidimensional data, developed at the Colorado State University Computer Science Department. This project is a fork of the original system developed by [Mathew Malensek](https://github.com/malensek/galileo) for geospatial datasets. Key features of this implementation include

* Creation and Deletion of Filesystems
* Dynamic Network Organization
* Customizable Spatio-Temporal Partitioning Scheme 
* Query Parallelism
* Result Grouping

## Requirements
* Java Runtime Version 1.7.0 or greater

## Setup on Network File System
[Download](https://github.com/jkachika/galileo-spacetime/archive/master.zip) the distribution and unzip it
    
    $ pwd
    /s/chopin/l/grad/jcharles/dev
    $ wget https://github.com/jkachika/galileo-spacetime/archive/master.zip
    $ unzip master.zip 
    $ mv galileo-spacetime-master galileo-spacetime
    $ cd galileo-spacetime
    $ ant
    $ rm config/network/*.group
    $ cd bin/util
    $ vi hostnames
    # Add list of hostname one on each line
    # 4 is the number of nodes per group in the below command
    $ mkgroups hostnames 4 
    $ mv *.group ../../config/network/

Setup environment settings in your `.bashrc` and `.profile` or `.bash_profile`
    
    $ vi .bashrc
    export GALILEO_HOME=/s/chopin/l/grad/jcharles/dev/galileo-spacetime
    # default network organization is read from this directory
    export GALILEO_CONF=/s/chopin/l/grad/jcharles/dev/galileo-spacetime/config
    # This is where all the galileo filesystems are stored
    export GALILEO_ROOT=/s/$(hostname)/a/tmp/$(whoami)/galileo
    
## Setup on Google Compute Engine (Debian 8 Jessie)

1. Setup passwordless SSH between the machine you download the project and all other machines in the cluster
    
        $ pwd
        /home/jkachika
        $ ssh-keygen -t rsa -f ~/.ssh/id_rsa -C [USERNAME]
        $ cat ~/.ssh/id_rsa.pub

2. Copy all those contents and go to Cloud Console > Compute Engine > Metadata > SSH Keys
3. Paste the contents there and save the file.
4. Check if you're able to ssh into other compute engine instances without password.
5. Download the distribution

        $ pwd
        /home/jkachika
        $ wget https://github.com/jkachika/galileo-spacetime/archive/master.zip
        $ unzip master.zip 
        $ mv galileo-spacetime-master galileo-spacetime
        $ cd galileo-spacetime
        $ ant
        $ rm config/network/*.group
        $ cd bin/util
        $ vi hostnames
        # Add list of hostname one on each line
        # 4 is the number of nodes per group in the below command
        $ mkgroups hostnames 4 
        $ mv *.group ../../config/network/
        $ cp hostnames /home/jkachika/
        
6. Setup environment settings in your `.bashrc` and `.profile` on the main machine. Note that no process will run on the master yet we can start and stop all the machines from master with this configuration

        $ vi .bashrc
        # setup java home
        export JAVA_HOME=/usr/lib/jvm/java-8-oracle
        export GALILEO_HOME=/home/jkachika/galileo-spacetime
        # default network organization is read from this directory
        export GALILEO_CONF=/home/jkachika/galileo-spacetime/config
        # This is where all the galileo filesystems are stored
        export GALILEO_ROOT=/tmp/galileo
        $ vi .profile
        # Add the same settings
        
7. Copy the distribution to all other machines

        $ cd /home/jkachika
        $ galileo-spacetime/bin/galileo-copy.sh hostnames /home/jkachika/galileo-spacetime /home/jkachika 
        $ galileo-spacetime/bin/galileo-setenv.sh hostnames $JAVA_HOME /home/jkachika/galileo-spacetime 
        

## Operating the cluster
1. Add `galileo-spacetime/bin` to `PATH`

        $ export PATH=$PATH:/home/jkachika/galileo-spacetime/bin

2. Starting the cluster
        
        $ galileo-cluster -c start
        
3. Status of the cluster
        
        $ galileo-cluster -c status
        
4. Stopping the cluster
        
        $ galileo-cluster -c stop
        
## Usage
The examples below use features from the [NYC Yellow Taxi Data Dictionary](http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) for the [NYC Yellow Taxi Dataset](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

1. Instantiate a Connector to commnunicate with Galileo storage nodes. Unless you're using multiple threads to communicate with Galileo, you should use only one connector for all your communications.

  ```java
  import galileo.comm.Connector;

  Connector connector = new Connector();
  ```
  
2. Creating a filesystem

  ```java
  import galileo.comm.FilesystemAction;
  import galileo.comm.FilesystemRequest;
  import galileo.dataset.SpatialHint;
  import galileo.dataset.feature.FeatureType;
  import galileo.net.NetworkDestination;
  import galileo.util.Pair;

  List<Pair<String, FeatureType>> featureList = new ArrayList<>();
  featureList.add(new Pair<>("tpep_pickup_datetime", FeatureType.STRING));
  featureList.add(new Pair<>("tpep_dropoff_datetime", FeatureType.STRING));
  featureList.add(new Pair<>("passenger_count", FeatureType.INT));
  featureList.add(new Pair<>("trip_distance", FeatureType.FLOAT));
  featureList.add(new Pair<>("pickup_longitude", FeatureType.FLOAT));
  featureList.add(new Pair<>("pickup_latitude", FeatureType.FLOAT));
  featureList.add(new Pair<>("ratecodeid", FeatureType.INT));
  featureList.add(new Pair<>("dropoff_longitude", FeatureType.FLOAT));
  featureList.add(new Pair<>("dropoff_latitude", FeatureType.FLOAT));
  featureList.add(new Pair<>("payment_type", FeatureType.INT));
  featureList.add(new Pair<>("fare_amount", FeatureType.FLOAT));
  featureList.add(new Pair<>("extra", FeatureType.FLOAT));
  featureList.add(new Pair<>("mta_tax", FeatureType.FLOAT));
  featureList.add(new Pair<>("improvement_surcharge", FeatureType.FLOAT));
  featureList.add(new Pair<>("tip_amount", FeatureType.FLOAT));
  featureList.add(new Pair<>("tolls_amount", FeatureType.FLOAT));
  featureList.add(new Pair<>("total_amount", FeatureType.FLOAT));

  SpatialHint spatialHint = new SpatialHint("pickup_latitude", "pickup_longitude");

  FilesystemRequest fsRequest = new FilesystemRequest("nyc_yellow_taxi", FilesystemAction.CREATE, featureList, spatialHint);
  fsRequest.setNodesPerGroup(2);
	fsRequest.setPrecision(6);
	fsRequest.setTemporalType(TemporalType.DAY_OF_MONTH);

  NetworkDestination storageNode = new NetworkDestination("harrisburg.cs.colostate.edu", 5634);
  connector.publishEvent(storageNode, fsRequest);
  ```
  
3. Preparing raw data to store as blocks

  ```java
  import galileo.comm.StorageRequest;
  import galileo.comm.TemporalType;
  import galileo.dataset.Block;
  import galileo.dataset.Coordinates;
  import galileo.dataset.Metadata;
  import galileo.dataset.SpatialProperties;
  import galileo.dataset.TemporalProperties;
  import galileo.dataset.feature.Feature;
  import galileo.dataset.feature.FeatureSet;
  import galileo.dht.hash.TemporalHash;
  import galileo.util.GeoHash;

  String data ="2016-01-01 00:00:00,2016-01-01 00:18:30,2,5.52,-73.980117797851563,40.743049621582031,1,-73.913490295410156,40.763141632080078,2,19,0.5,0.5,0,0,0.3,20.3\n"
				+ "2016-01-01 00:00:00,2016-01-01 00:26:45,2,7.45,-73.994056701660156,40.719989776611328,1,-73.966361999511719,40.789871215820313,2,26,0.5,0.5,0,0,0.3,27.3\n"
				+ "2016-01-01 00:00:01,2016-01-01 00:11:55,1,1.20,-73.979423522949219,40.744613647460938,1,-73.992034912109375,40.753944396972656,2,9,0.5,0.5,0,0,0.3,10.3\n"
				+ "2016-01-01 00:00:02,2016-01-01 00:11:14,1,6.00,-73.947151184082031,40.791046142578125,1,-73.920768737792969,40.865577697753906,2,18,0.5,0.5,0,0,0.3,19.3\n"
				+ "2016-01-01 00:00:02,2016-01-01 00:11:08,1,3.21,-73.998344421386719,40.723896026611328,1,-73.995849609375,40.688400268554688,2,11.5,0.5,0.5,0,0,0.3,12.8\n"
				+ "2016-01-01 00:00:03,2016-01-01 00:06:19,1,.79,-74.006149291992188,40.744918823242188,1,-73.993797302246094,40.741439819335938,2,6,0.5,0.5,0,0,0.3,7.3\n"
				+ "2016-01-01 00:00:03,2016-01-01 00:15:49,6,2.43,-73.969329833984375,40.763538360595703,1,-73.995689392089844,40.744251251220703,1,12,0.5,0.5,3.99,0,0.3,17.29\n"
				+ "2016-01-01 00:00:03,2016-01-01 00:00:11,4,.01,-73.989021301269531,40.721538543701172,1,-73.988960266113281,40.721698760986328,2,2.5,0.5,0.5,0,0,0.3,3.8\n"
				+ "2016-01-01 00:00:04,2016-01-01 00:14:32,1,3.70,-74.004302978515625,40.742240905761719,1,-74.007362365722656,40.706935882568359,1,14,0.5,0.5,3.05,0,0.3,18.35\n";
  String[] records = data.split("\\r?\\n");
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  sdf.setTimeZone(TemporalHash.TIMEZONE);
  Map<String, StringBuffer> blockMap = new HashMap<>();
  Map<String, Float> minimumAmount = new HashMap<>();
  Map<String, Float> maximumAmount = new HashMap<>();
  Map<String, Float> meanAmount = new HashMap<>();
  Map<String, Long> timestamps = new HashMap<>();
  Map<String, Coordinates> locations = new HashMap<>();
  for(String record : records){
    String[] fields = record.split(",");
    String hash = GeoHash.encode(Float.parseFloat(fields[5]), Float.parseFloat(fields[4]), 6);
    String blockKey = String.format("%s-%s", fields[0].split("\\s")[0], hash);
    StringBuffer blockBuffer = blockMap.get(blockKey);
    float totalAmount = Float.parseFloat(fields[16]);
    if(blockBuffer == null){
      blockBuffer = new StringBuffer();
      blockMap.put(blockKey, blockBuffer);
      blockBuffer.append(record);
      minimumAmount.put(blockKey, totalAmount);
      maximumAmount.put(blockKey, totalAmount);
      meanAmount.put(blockKey, totalAmount);
      timestamps.put(blockKey, sdf.parse(fields[0]).getTime());
      locations.put(blockKey, new Coordinates(Float.parseFloat(fields[5]), Float.parseFloat(fields[4])));
    } else {
      blockBuffer.append("\n");
      blockBuffer.append(record);
      if(totalAmount < minimumAmount.get(blockKey))
        minimumAmount.put(blockKey, totalAmount);
      if(totalAmount > maximumAmount.get(blockKey))
        maximumAmount.put(blockKey, totalAmount);
      meanAmount.put(blockKey, totalAmount + meanAmount.get(blockKey));
    }
  }
  ```

4. Storing the blocks

  ```java
  for (String blockKey : blockMap.keySet()) {
    StringBuffer blockData = blockMap.get(blockKey);
    TemporalProperties temporalProperties = new TemporalProperties(timestamps.get(blockKey));
    SpatialProperties spatialProperties = new SpatialProperties(locations.get(blockKey));
    FeatureSet attributes = new FeatureSet();
    Metadata metadata = new Metadata();
    attributes.put(new Feature("min_amount", minimumAmount.get(blockKey)));
    attributes.put(new Feature("mean_amount", meanAmount.get(blockKey) / numTrips.get(blockKey)));
    attributes.put(new Feature("max_amount", maximumAmount.get(blockKey)));
    attributes.put(new Feature("num_trips", numTrips.get(blockKey)));
    metadata.setName(blockKey);
    metadata.setSpatialProperties(spatialProperties);
    metadata.setTemporalProperties(temporalProperties);
    metadata.setAttributes(attributes);
    StorageRequest storageRequest = new StorageRequest(
        new Block("nyc_yellow_taxi", metadata, blockData.toString().getBytes("UTF-8")));
    connector.publishEvent(storageNode, storageRequest);
  }
  ```
