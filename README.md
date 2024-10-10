# Evidence S3 Source Plugin

Install this plugin in an Evidence app with
```bash
npm install evidence-connector-s3
```

Register the plugin in your project in your evidence.plugins.yaml file with
```bash
datasources:
  evidence-connector-s3: {}
```

Launch the development server with `npm run dev` and navigate to the settings menu (localhost:3000/settings) to add a data source using this plugin.


## Source Options

| Option | Description |
| --- | --- |
| accessKeyId | Access Key ID for the AWS S3 bucket. |
| secretAccessKey | Secret Access Key for the AWS S3 bucket. |
| region | Region for the AWS S3 bucket. |

## Source Queries

Source queries are expected to be in the form of a SQL file with the extension .sql.
Use DuckDB syntax for reading files from S3.

```sql
from 's3://<bucket>/<path>.<format>'
```

Where format is one of Parquet, CSV, or ORC.

## S3 Configuration

You will need to create or use an IAM user with an Access Key.

Your user should have an access policy that allows DuckDB to read the files in the bucket.

Example policy for a user, accessing a bucket called `my-s3-bucket`

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"s3:ListBucket",
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject",
				"s3:AbortMultipartUpload"
			],
			"Resource": [
				"arn:aws:s3:::my-s3-bucket",
				"arn:aws:s3:::my-s3-bucket/*"
			]
		}
	]
}
```
