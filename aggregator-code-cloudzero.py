# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
from datetime import datetime, timedelta
import requests
import time

number_of_hours = int(os.environ['NUMBER_OF_HOURS'])
cloudzero_api_key=os.environ['CLOUDZERO_API_KEY']

cloudformation = boto3.client('cloudformation')
logs = boto3.client('logs')

def aggregate_lambda_invocations_by_tenant(event, context):
   log_group_names = []
   log_group_prefix = '/aws/lambda/'
   cloudformation_paginator = cloudformation.get_paginator('list_stack_resources')
   response_iterator = cloudformation_paginator.paginate(StackName='stack-pooled')
    for stack_resources in response_iterator:
        for resource in stack_resources['StackResourceSummaries']:
            if (resource["LogicalResourceId"] == "CreateOrderFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]), log_group_names)
                continue
            if (resource["LogicalResourceId"] == "CreateProductFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]), log_group_names)
                continue
            if (resource["LogicalResourceId"] == "GetOrdersFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]), log_group_names)
                continue
            if (resource["LogicalResourceId"] == "GetProductsFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]), log_group_names)
                continue 
   time_zone = datetime.now().astimezone().tzinfo
   start_time = int((datetime.now(tz=time_zone) - timedelta(hours=number_of_hours)).timestamp())
   end_time = int(time.time())

  
   query_string='fields @timestamp, @message \
       | filter @message like /Request completed/ \
       | fields tenant_id as TenantId, service as Service \
       | stats count (TenantId) as LambdaInvocation by TenantId, Service, dateceil(@timestamp, 1d) as timestamp'
   query_results = __query_cloudwatch_logs(logs, log_group_names, query_string, start_time, end_time)

   __post_telemetry_records("LambdaInvocation", query_results["results"])



def aggregate_dynamodb_capacity_units_by_tenant(event, context):
   product_service_log_group_names = []
   order_service_log_group_names = []
   log_group_prefix = '/aws/lambda/'
   cloudformation_paginator = cloudformation.get_paginator('list_stack_resources')
   response_iterator = cloudformation_paginator.paginate(StackName='stack-pooled')
    for stack_resources in response_iterator:
        for resource in stack_resources['StackResourceSummaries']:
            if (resource["LogicalResourceId"] == "CreateProductFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                product_service_log_group_names)
                continue   
            if (resource["LogicalResourceId"] == "UpdateProductFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                product_service_log_group_names)
                continue
            if (resource["LogicalResourceId"] == "GetProductsFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                product_service_log_group_names)
                continue    
            if (resource["LogicalResourceId"] == "CreateOrderFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                order_service_log_group_names)
                continue
            if (resource["LogicalResourceId"] == "UpdateOrderFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                order_service_log_group_names)
                continue
            if (resource["LogicalResourceId"] == "GetOrdersFunction"):
                __add_log_group_name(logs, ''.join([log_group_prefix,resource["PhysicalResourceId"]]),
                order_service_log_group_names)
                continue
              
  
   time_zone = datetime.now().astimezone().tzinfo
   start_time = int((datetime.now(tz=time_zone) - timedelta(hours=number_of_hours)).timestamp())
   end_time = int(time.time())


   product_service_capacity_units_query = 'fields @timestamp, @message \
   | filter @message like /ProductCreated|ProductUpdated|ProductsRetrieved/ \
   | fields tenant_id as TenantId, service as Service, \
   ProductCreated.0 as ProductCreated, ProductUpdated.0 as ProductUpdated, \
   ProductsRetrieved.0*0.5 as ProductsRetrieved \
   | display coalesce(ProductCreated, ProductUpdated,  ProductsRetrieved) as ProductUnits \
   | stats sum(ProductUnits) as CapacityUnits by TenantId, Service, dateceil(@timestamp, 1d) as timestamp'
   product_service_capacity_units = __query_cloudwatch_logs(logs, product_service_log_group_names,
   product_service_capacity_units_query, start_time, end_time)

  

   order_service_capacity_units_query = 'fields @timestamp, @message \
       | filter @message like /OrderCreated|OrderUpdated|OrdersRetrieved/ \
       | fields tenant_id as TenantId, service as Service, \
       OrderCreated.0 as OrderCreated, OrderUpdated.0 as OrderUpdated, \
       OrdersRetrieved.0*0.5 as OrdersRetrieved \
       | display coalesce(OrderCreated, OrderUpdated,  OrdersRetrieved) as OrderUnits \
       | stats sum(OrderUnits) as CapacityUnits by TenantId, Service, dateceil(@timestamp, 1d) as timestamp'
   order_service_capacity_units = __query_cloudwatch_logs(logs, order_service_log_group_names,
   order_service_capacity_units_query, start_time, end_time)
  

   __post_telemetry_records("CapacityUnits", product_service_capacity_units["results"])
   __post_telemetry_records("CapacityUnits", order_service_capacity_units["results"])


def __post_telemetry_records(payload_type, payload):

   if not payload:
       return "Payload is empty, so skipped posting to Cloudzero !!"
   else:
       url = "https://api.cloudzero.com/unit-cost/v1/telemetry"
       headers = {"Content-Type": "application/json", "Authorization": cloudzero_api_key}
  
       formatted_payload = __convert_payload(payload_type, payload)

       response = requests.request("POST", url, json=formatted_payload, headers=headers, timeout=5)

       return response   

def __convert_payload(payload_type, payload):
   records = []
   transformed_payload = {}
   for result in payload:
       transformed_payload[result[0]["field"]] = result[0]["value"]
       transformed_payload[result[1]["field"]] = result[1]["value"]
       transformed_payload[result[2]["field"]] = result[2]["value"]
       transformed_payload[result[3]["field"]] = result[3]["value"]
      
       format_record = __cloudzero_payload_format(payload_type, transformed_payload) 
       
       if not format_record:
            continue
        else:
            records.append(__cloudzero_payload_format(payload_type, transformed_payload))

   return dict({"records":records})     

def __cloudzero_payload_format(payload_type, transformed_payload):

    if not payload_type or not transformed_payload:
        return    

   if payload_type == 'LambdaInvocation':
       return {
           "value" : transformed_payload["LambdaInvocation"],
           "timestamp" : transformed_payload["timestamp"],
           "granularity": "DAILY",
           "element-name" : transformed_payload["TenantId"],
           "telemetry-stream" : "Pooled-LambdaInvocation",
           "filter": {
               "custom:Architecture Services": [
                   "Application Service Pooled"
               ]
           }

       }

   elif payload_type == 'CapacityUnits':
       return {
           "value" : transformed_payload["CapacityUnits"],
           "timestamp" : transformed_payload["timestamp"],
           "granularity": "DAILY",
           "element-name" : transformed_payload["TenantId"],
           "telemetry-stream" : "Pooled-CapacityUnits",
           "filter": {
               "service": [
                   "AmazonDynamoDB"
               ],
               "custom:Architecture Services": [
                   "Application Service Pooled"
               ]
           }

       }
  
def __query_cloudwatch_logs(logs, log_group_names, query_string, start_time, end_time):
   query = logs.start_query(logGroupNames=log_group_names,
   startTime=start_time,
   endTime=end_time,
   queryString=query_string)

   query_results = logs.get_query_results(queryId=query["queryId"])

   while query_results['status']=='Running' or query_results['status']=='Scheduled':
       time.sleep(5)
       query_results = logs.get_query_results(queryId=query["queryId"])

   return query_results

def __is_log_group_exists(logs_client, log_group_name):
    logs_paginator = logs_client.get_paginator('describe_log_groups')
    response_iterator = logs_paginator.paginate(logGroupNamePrefix=log_group_name)
    for log_groups_list in response_iterator:
        if not log_groups_list["logGroups"]:
            return False
        else:
            return True      

def __add_log_group_name(logs_client, log_group_name, log_group_names_list):

   if __is_log_group_exists(logs_client, log_group_name):
       log_group_names_list.append(log_group_name)   
