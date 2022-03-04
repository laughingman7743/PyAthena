Benchmarks
==========

Environment
-----------

Amazon Linux 2
~~~~~~~~~~~~~~

.. code:: bash

   $ uname -a
   Linux ip-xxx-xxx-xxx-xxx.us-west-2.compute.internal 4.14.123-111.109.amzn2.x86_64 #1 SMP Mon Jun 10 19:37:57 UTC 2019 x86_64 x86_64 x86_64 GNU/Linux

* Instance types

  * r5.large
  * vCPU: 2
  * Memory: 16
  * Networking Performance: Up to 10 Gigabit

* Install packages

  .. code:: bash

     $ sudo yum install java-1.8.0-openjdk python3 python3-pip git
     $ sudo pip3 install pipenv

* Python

  Python 3.7

  .. code:: bash

     $ python3 -V
     Python 3.7.3

* Java

  OpenJDK 8

  .. code:: bash

     $ java -version
     openjdk version "1.8.0_201"
     OpenJDK Runtime Environment (build 1.8.0_201-b09)
     OpenJDK 64-Bit Server VM (build 25.201-b09, mixed mode)

Table
-----

Download statistics from the Python Package Index.

https://bigquery.cloud.google.com/dataset/the-psf:pypi

* Date:

  2018-09-15

* Format:

  Avro (Export from BigQuery to GCS in Avro format and upload it to S3)

* Schema:

  pypi_downloads_20180915.sql

* Size:

  12,746,274,716 Bytes (11.9 GiB)

* Objects:

  81 Objects

* Rows:

  36,757,860

Results
-------

Large result sets
~~~~~~~~~~~~~~~~~

* Query:

  .. code:: sql

     SELECT
       *
     FROM
       pypi_downloads_20180915
     WHERE
       file.project = 'pip'

* Size:

  1,010,013,072 Bytes (963.2 MiB)

* Rows:

  1,923,322

* Results:

  .. code:: text

     PyAthenaJDBC Cursor ===========================
     loop:0	count:1923322	elasped:282.6379404067993
     loop:1	count:1923322	elasped:284.39211225509644
     loop:2	count:1923322	elasped:278.96245217323303
     loop:3	count:1923322	elasped:285.5322263240814
     loop:4	count:1923322	elasped:282.21576499938965
     Avg: 282.74809923171995
     ===============================================

     PyAthena Cursor ===============================
     loop:0	count:1923322	elasped:484.90504693984985
     loop:1	count:1923322	elasped:502.2120985984802
     loop:2	count:1923322	elasped:523.4487774372101
     loop:3	count:1923322	elasped:504.15862131118774
     loop:4	count:1923322	elasped:515.554434299469
     Avg: 506.0557957172394
     ===============================================

     PyAthena PandasCursor =========================
     loop:0	count:1923322	elasped:55.4522430896759
     loop:1	count:1923322	elasped:53.483012676239014
     loop:2	count:1923322	elasped:49.42951560020447
     loop:3	count:1923322	elasped:54.449145793914795
     loop:4	count:1923322	elasped:55.516170501708984
     Avg: 53.66601753234863
     ===============================================

Medium result sets
~~~~~~~~~~~~~~~~~~

* Query:

  .. code:: sql

     SELECT
       *
     FROM
       pypi_downloads_20180915
     WHERE
       file.project = 'requests'

* Size:

  286,558,367 Bytes (273.3 MiB)

* Rows:

  495,730

* Results:

  .. code:: text

     PyAthenaJDBC Cursor ===========================
     loop:0	count:495730	elasped:80.66084718704224
     loop:1	count:495730	elasped:82.11357545852661
     loop:2	count:495730	elasped:78.96936511993408
     loop:3	count:495730	elasped:136.36216259002686
     loop:4	count:495730	elasped:79.34635043144226
     Avg: 91.49046015739441
     ===============================================

     PyAthena Cursor ===============================
     loop:0	count:495730	elasped:133.82753896713257
     loop:1	count:495730	elasped:135.11225271224976
     loop:2	count:495730	elasped:136.79930639266968
     loop:3	count:495730	elasped:137.63540840148926
     loop:4	count:495730	elasped:134.88309502601624
     Avg: 135.6515202999115
     ===============================================

     PyAthena PandasCursor =========================
     loop:0	count:495730	elasped:18.725481033325195
     loop:1	count:495730	elasped:20.726996898651123
     loop:2	count:495730	elasped:19.722756385803223
     loop:3	count:495730	elasped:23.745840787887573
     loop:4	count:495730	elasped:20.753735065460205
     Avg: 20.734962034225465
     ===============================================

Small result sets
~~~~~~~~~~~~~~~~~

* Query:

  .. code:: sql

     SELECT
       *
     FROM
       pypi_downloads_20180915
     WHERE
       file.project = 'pyhive'

* Size:

  5,074,402 Bytes (4.8 MiB)

* Rows:

  9,152

* Results:

  .. code:: text

     PyAthenaJDBC Cursor ===========================
     loop:0	count:9152	elasped:15.812973737716675
     loop:1	count:9152	elasped:15.872679233551025
     loop:2	count:9152	elasped:14.720329523086548
     loop:3	count:9152	elasped:13.776614665985107
     loop:4	count:9152	elasped:12.917945623397827
     Avg: 14.620108556747436
     ===============================================

     PyAthena Cursor ===============================
     loop:0	count:9152	elasped:16.516400814056396
     loop:1	count:9152	elasped:14.492462396621704
     loop:2	count:9152	elasped:14.72799015045166
     loop:3	count:9152	elasped:14.450309753417969
     loop:4	count:9152	elasped:15.605662107467651
     Avg: 15.158565044403076
     ===============================================

     PyAthena PandasCursor =========================
     loop:0	count:9152	elasped:12.77013611793518
     loop:1	count:9152	elasped:12.562756061553955
     loop:2	count:9152	elasped:10.477757930755615
     loop:3	count:9152	elasped:17.702434301376343
     loop:4	count:9152	elasped:14.664473056793213
     Avg: 13.635511493682861
     ===============================================
