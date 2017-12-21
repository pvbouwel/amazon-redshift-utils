# Tests

## Cloudformation tests

The cloudformation directory contains an AWS Cloudformation stack (located in `RedshiftCFTemplate.json`) that will 
perform integration tests for the Unload Copy Utility which can verify that a git repository has code that is working 
as expected.  The stack takes numerous parameters which are documented inside the stack for most cases the default 
values will suffice.  The stack will spawn up 2 clusters one from snapshot and one empty cluster.  It will also create
 an EC2 instance which will be bootstrapped to be a client host for the Copy Unload utility and on which the different
tests can run.  This instance will run the different scenario's one by one and log the output.  From this log output a 
test report can be build which will show how the code behaves.  The scenarios are examples of how Unload Copy utility 
can be issued and can in that respect act as documentation.

### Pre-requisites

In order to run the Cloudformation stack you need a few requirements set up this section goes through them:

#### SourceClusterSnapshot

The parameter `SourceClusterSnapshot` should point to a snapshot that contains the tables that are used in the 
scenario's.  The file `bootstrap_source.sql` contains SQL on how to create such a cluster starting from an empty
cluster (you will need to replace `<arn-of-your-copy-role>` with the ARN of a role that is associated with your 
cluster and which is allowed to use S3).

#### ReportBucket

The parameter `ReportBucket` should be the name of a bucket that is in the same region as where you will spawn the 
stack.  After all the tests have run the script on the EC2 instance will push log files to this bucket.  In order for 
this to work the user that will spawn up the stack needs to be able to create an IAM role that allows access to this 
bucket.


## Other tests
Other files ending in tests.py will contain tests that are Python implemented using the TestCase class of the unittest 
library.  Unittest will require Python 3 to run.

| Filename                                 | Description |
| ---------------------------------------- | ----------- |
| global_config_unittests.py               | Unittests to validate the functionality of global_config |
| redshift_unload_copy_regression_tests.py | Tests to follow up with behavior while changing internals to more classes |
| redshift_unload_copy_unittests.py        | Group of simple unittests that do not have their own group |
