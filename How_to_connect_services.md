Setting up IAM roles for accessing Redshift and Bedrock involves a few steps. Here’s a general guide:

For Redshift:

Create an IAM Role:

Go to the IAM console in AWS.


Create a new role and choose “AWS service” as the type of trusted entity.


Select “Redshift” as the service that will use this role.



Attach Policies:

Attach policies that grant the necessary permissions for accessing Redshift. For example, AmazonRedshiftReadOnlyAccess or a custom policy with specific permissions.



Trust Relationship:

Edit the trust relationship of the role to allow Redshift to assume the role.



Associate the IAM Role with Redshift:

Go to your Redshift cluster and associate the created IAM role with it.



For Bedrock API:

Create an IAM Role:

Again, go to the IAM console.


Create a new role and choose “AWS service” as the trusted entity.


Select “Bedrock” as the service that will use this role.



Attach Policies:

Attach the necessary policies that allow making API calls to Bedrock. This might include policies like AmazonBedrockFullAccess or a custom policy with required actions.



Trust Relationship:

Edit the trust relationship to allow the service (e.g., your application) to assume the role.



Use the Role in Your Application:

In your Streamlit app, configure boto3 to use this IAM role for making Bedrock API calls.



Make sure to follow the principle of least privilege by only granting necessary permissions. Let me know if you need more details on any step!