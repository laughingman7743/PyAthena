.. _testing:

Testing
=======

Environment variables
---------------------

Depends on the following environment variables:

.. code:: bash

    $ export AWS_DEFAULT_REGION=us-west-2
    $ export AWS_ATHENA_S3_STAGING_DIR=s3://YOUR_S3_BUCKET/path/to/
    $ export AWS_ATHENA_WORKGROUP=pyathena
    $ export AWS_ATHENA_SPARK_WORKGROUP=pyathena-spark

In addition, you need to create a workgroup with the `Query result location` set to the name specified in the `AWS_ATHENA_WORKGROUP` environment variable.
If primary is not available as the default workgroup, specify an alternative workgroup name for the default in the environment variable `AWS_ATHENA_DEFAULT_WORKGROUP`.

.. code:: bash

    $ export AWS_ATHENA_DEFAULT_WORKGROUP=DEFAULT_WORKGROUP

Run test
--------

.. code:: bash

    $ pip install hatch or pipx install hatch or brew install hatch
    $ hatch run test
    $ hatch run test-sqla

Run test multiple Python versions
---------------------------------

.. code:: bash

    $ pip install hatch or pipx install hatch or brew install hatch
    $ hatch -e test run test
    $ hatch -e test run test-sqla

Code formatting
---------------

The code formatting uses `ruff`_.

Appy format
~~~~~~~~~~~

.. code:: bash

    $ make fmt

Check format
~~~~~~~~~~~~

.. code:: bash

    $ make chk

GitHub Actions
--------------

GitHub Actions uses OpenID Connect (OIDC) to access AWS resources. You will need to refer to the `GitHub Actions documentation`_ to configure it.

The CloudFormation templates for creating GitHub OIDC Provider and IAM Role can be found in the `aws-actions/configure-aws-credentials repository`_.

Under `scripts/cloudformation`_ you will also find a CloudFormation template with additional permissions and workgroup settings needed for testing.

The example of the CloudFormation execution command is the following:

.. code:: bash

    $ aws --region us-west-2 \
        cloudformation create-stack \
        --stack-name github-actions-oidc-pyathena \
        --capabilities CAPABILITY_NAMED_IAM \
        --template-body file://./scripts/cloudformation/github_actions_oidc.yaml \
        --parameters ParameterKey=GitHubOrg,ParameterValue=laughingman7743 \
          ParameterKey=RepositoryName,ParameterValue=PyAthena \
          ParameterKey=BucketName,ParameterValue=laughingman7743-athena \
          ParameterKey=RoleName,ParameterValue=github-actions-oidc-pyathena-test \
          ParameterKey=WorkGroupName,ParameterValue=pyathena-test

.. _`scripts/cloudformation`: https://github.com/laughingman7743/PyAthena/tree/master/scripts/cloudformation
.. _`ruff`: https://github.com/astral-sh/ruff
.. _`GitHub Actions documentation`: https://docs.github.com/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-amazon-web-services
.. _`aws-actions/configure-aws-credentials repository`: https://github.com/aws-actions/configure-aws-credentials#sample-iam-role-cloudformation-template
