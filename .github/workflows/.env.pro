AWS_REGION=us-east-1
AWS_S3_BUCKET=klg-klaanayticsprod
MASTER_TABLE_SCRIPTS_PATH=LH12/model_pkl/aws-glue-master-table-scripts
AWS_PAGER=""

JOB_NAME=MasterTableLH12
TRIGGER_NAME=LH12MasterTableTrigger
TRIGGER_TYPE=SCHEDULED
IS_LOCAL=False
IS_ATHENA=True
EMAIL_LAMBDA_FUNCTION=arn:aws:lambda:us-east-1:949692552910:function:lh12-sendEmail
TRIGGER_CRONJOB=cron(00 20 ? * WED *)
