Benchmarks
==========

Environment
-----------

Amazon Linux 2
~~~~~~~~~~~~~~

.. code:: bash

   $ uname -a
   Linux ip-xxx-xxx-xxx-xxx.us-west-2.compute.internal 5.10.96-90.460.amzn2.x86_64 #1 SMP Fri Feb 4 17:12:04 UTC 2022 x86_64 x86_64 x86_64 GNU/Linux

* Instance types

  * r5.large
  * vCPU: 2
  * Memory: 16
  * Networking Performance: Up to 10 Gigabit

* Install packages

  .. code:: bash

     $ sudo yum install git
     $ sudo pip3 install poetry

* Python

  Python 3.7

  .. code:: bash

     $ python3 -V
     Python 3.7.10

Table
-----

Download statistics and metadata from the Python Package Index.

https://console.cloud.google.com/marketplace/product/gcp-public-data-pypi/pypi

* Date:

  2022-02-01

* Format:

  Parquet with Snappy (Export from BigQuery to GCS in Parquet format and upload it to S3)

* Schema:

  pypi_downloads_20220201.sql

* Size:

  18.5 GB

* Objects:

  500 Objects

* Rows:

  492,060,414

Results
-------

Large result sets
~~~~~~~~~~~~~~~~~

* Query:

  .. code:: sql

     SELECT
       *
     FROM
       file_downloads_20220201
     WHERE
       project = 'pip'

* Size:

  2,843,119,182 Bytes (2.6 GiB)

* Rows:

  5,036,231

* Results:

  .. code:: text

     #254 branch PandasCursor =========================
     loop:0	count:5036231	elasped:183.2605962753296
     loop:1	count:5036231	elasped:189.2886574268341
     loop:2	count:5036231	elasped:185.185964345932
     loop:3	count:5036231	elasped:180.66826105117798
     loop:4	count:5036231	elasped:188.4237265586853
     Avg: 185.3654411315918
     ===============================================

     master branch (2.4.1) PandasCursor =========================
     loop:0	count:5036231	elasped:471.96120595932007
     loop:1	count:5036231	elasped:471.62245082855225
     loop:2	count:5036231	elasped:474.5827896595001
     loop:3	count:5036231	elasped:469.4493181705475
     loop:4	count:5036231	elasped:470.50114393234253
     Avg: 471.6233817100525
     ===============================================

Medium result sets
~~~~~~~~~~~~~~~~~~

* Query:

  .. code:: sql

     SELECT
       *
     FROM
       file_downloads_20220201
     WHERE
       project = 'tenacity'

* Size:

  456,601,182 Bytes (435 MiB)

* Rows:

  770,019

* Results:

  .. code:: text

     #254 branch PandasCursor =========================
     loop:0	count:770019	elasped:32.215473651885986
     loop:1	count:770019	elasped:32.146485328674316
     loop:2	count:770019	elasped:32.11235213279724
     loop:3	count:770019	elasped:31.1186785697937
     loop:4	count:770019	elasped:31.295124292373657
     Avg: 31.777622795104982
     ===============================================

     master branch (2.4.1) PandasCursor =========================
     loop:0	count:770019	elasped:83.47699689865112
     loop:1	count:770019	elasped:75.81913661956787
     loop:2	count:770019	elasped:76.54475831985474
     loop:3	count:770019	elasped:75.42293787002563
     loop:4	count:770019	elasped:75.78199934959412
     Avg: 77.4091658115387
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
       project = 'pyhive'

* Size:

  40,797,849 Bytes (39 MiB)

* Rows:

  71,747

* Results:

  .. code:: text

     #254 branch PandasCursor =========================
     loop:0	count:71747	elasped:8.058117151260376
     loop:1	count:71747	elasped:7.99663519859314
     loop:2	count:71747	elasped:8.116108655929565
     loop:3	count:71747	elasped:7.994446039199829
     loop:4	count:71747	elasped:8.99520206451416
     Avg: 8.232101821899414
     ===============================================

     master branch (2.4.1) PandasCursor =========================
     loop:0	count:71747	elasped:11.166686296463013
     loop:1	count:71747	elasped:11.104575634002686
     loop:2	count:71747	elasped:11.057553052902222
     loop:3	count:71747	elasped:12.151265382766724
     loop:4	count:71747	elasped:12.126625061035156
     Avg: 11.52134108543396
     ===============================================
